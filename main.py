import os
import requests
import pandas as pd

# =========================
# CONFIGURATION
# =========================
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")  # Set in GitHub Secrets or locally
OUTPUT_DIR = "hubspot_data"

# List of HubSpot objects to fetch
OBJECTS = {
    "carts": "crm/v3/objects/carts",
    "companies": "crm/v3/objects/companies",
    "contacts": "crm/v3/objects/contacts",
    "deals": "crm/v3/objects/deals",
    "discounts": "crm/v3/objects/discounts",
    "fees": "crm/v3/objects/fees",
    "goals": "crm/v3/objects/goals",
    "invoices": "crm/v3/objects/invoices",
    "leads": "crm/v3/objects/leads",
    "line_items": "crm/v3/objects/line_items",
    "orders": "crm/v3/objects/orders",
    "partner_clients": "crm/v3/objects/partner_clients",
    "partner_services": "crm/v3/objects/partner_services",
    "payments": "crm/v3/objects/payments",
    "products": "crm/v3/objects/products",
    "quotes": "crm/v3/objects/quotes",
    "taxes": "crm/v3/objects/taxes",    
    "tickets": "crm/v3/objects/tickets"
}

# =========================
# FETCH FUNCTION
# =========================
def fetch_all_objects(object_name, endpoint):
    """
    Fetch all records for a given HubSpot object type in batches of 1000.
    """
    url = f"https://api.hubapi.com/{endpoint}"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}"
    }
    params = {
        "limit": 1000,  # Maximum allowed per request for most HubSpot CRM APIs
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
        print(f"📄 {object_name}: Fetched batch {page_count} ({len(results)} records)")

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
        records = fetch_all_objects(obj, endpoint)

        # Convert to DataFrame
        df = pd.json_normalize(records)

        # Save to CSV
        output_path = os.path.join(OUTPUT_DIR, f"{obj}.csv")
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"💾 Saved: {output_path}")

    print("🎯 All objects fetched successfully.")