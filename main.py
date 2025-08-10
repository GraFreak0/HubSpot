import os
import requests
import pandas as pd
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIGURATION
# =========================
load_dotenv()
ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("Please set the HUBSPOT_ACCESS_TOKEN environment variable.")

OUTPUT_DIR = "hubspot_data"
MAX_WORKERS = 8  # Parallel threads

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
    url = f"https://api.hubapi.com/{endpoint}"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "accept": "application/json"
    }
    params = {
        "limit": 100,
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
    return object_name, all_results

# =========================
# MAIN SCRIPT (PARALLEL)
# =========================
if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_all_objects, obj, endpoint): obj for obj, endpoint in OBJECTS.items()}

        for future in as_completed(futures):
            obj = futures[future]
            try:
                obj_name, records = future.result()
                if records:
                    df = pd.json_normalize(records)
                    output_path = os.path.join(OUTPUT_DIR, f"{obj_name}.csv")
                    df.to_csv(output_path, index=False, encoding="utf-8")
                    print(f"💾 Saved: {output_path}")
                else:
                    print(f"⚠️ No records found for {obj_name}")
            except requests.exceptions.HTTPError as e:
                print(f"❌ Error fetching {obj}: {e}")

    print("🎯 All objects fetched successfully.")