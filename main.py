import os
import requests
import pandas as pd
import clickhouse_connect
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIGURATION
# =========================
load_dotenv()

ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
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
    "goal_targets": "crm/v3/objects/goal_targets",
    "invoices": "crm/v3/objects/invoices",
    # "leads": "crm/v3/objects/leads",
    "line_items": "crm/v3/objects/line_items",
    "orders": "crm/v3/objects/orders",
    # "partner_clients": "crm/v3/objects/partner_clients",
    # "partner_services": "crm/v3/objects/partner_services",
    "payments": "crm/v3/objects/commerce_payments",
    "products": "crm/v3/objects/products",
    "quotes": "crm/v3/objects/quotes",
    "taxes": "crm/v3/objects/taxes",    
    # "tickets": "crm/v3/objects/tickets",
    "calls": "crm/v3/objects/calls",
    "emails": "crm/v3/objects/emails",
    "meetings": "crm/v3/objects/meetings",
    "notes": "crm/v3/objects/notes",
    "tasks": "crm/v3/objects/tasks",
    "communications": "crm/v3/objects/communications",
    # "conversations": "conversations/v3/conversations/inboxes",
    # "feedback_submissions": "crm/v3/objects/feedback_submissions",
    "postal_mail": "crm/v3/objects/postal_mail",
}


# =========================
# CLICKHOUSE CLIENT
# =========================
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    # port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    secure=True,  # Set to True if using HTTPS
)

import pandas as pd
import clickhouse_connect

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    secure=True,
)

def create_table_if_not_exists(table_name, df):
    """Create ClickHouse table if it doesn't exist, with columns based on DataFrame dtypes."""
    columns = []
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            col_type = "Int64"
        elif pd.api.types.is_float_dtype(dtype):
            col_type = "Float64"
        else:
            col_type = "String"
        columns.append(f"`{col}` {col_type}")

    columns_sql = ", ".join(columns)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {columns_sql}
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    """
    client.command(create_sql)

def insert_into_clickhouse(table_name, df):
    """Insert DataFrame data into ClickHouse, replacing None/NaN with empty strings."""
    # Replace None/NaN in object/string columns with empty string
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].fillna("")
    
    create_table_if_not_exists(table_name, df)
    client.insert_df(table_name, df)

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

                    # Save to CSV
                    output_path = os.path.join(OUTPUT_DIR, f"{obj_name}.csv")
                    df.to_csv(output_path, index=False, encoding="utf-8")
                    print(f"💾 Saved: {output_path}")

                    # Insert into ClickHouse
                    insert_into_clickhouse(obj_name, df)
                    print(f"📦 Loaded into ClickHouse table: {obj_name}")

                else:
                    print(f"⚠️ No records found for {obj_name}")
            except requests.exceptions.HTTPError as e:
                print(f"❌ Error fetching {obj}: {e}")

    print("🎯 All objects fetched and loaded successfully.")