#!/usr/bin/env python3
"""
load_data_fixed.py
Improved HubSpot -> CSV -> ClickHouse loader with:
 - per-thread ClickHouse client
 - per-thread requests.Session with retries & backoff
 - graceful handling of 403/400 errors
 - chunked properties requests to avoid very long URLs
 - safer pagination & merging of properties per record
"""

import os
import time
import json
from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import clickhouse_connect
from dotenv import load_dotenv
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# =========================
# CONFIGURATION
# =========================
load_dotenv()

ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
OUTPUT_DIR = "hubspot_data"

# Tune these
MAX_WORKERS = 4
REQUEST_TIMEOUT = 120         # seconds
SESSION_TOTAL_RETRIES = 5
SESSION_BACKOFF_FACTOR = 1.0
PROPERTIES_CHUNK_SIZE = 50    # shrink if HubSpot still errors or properties very large
PAGE_LIMIT = 100              # HubSpot max page size

if not ACCESS_TOKEN:
    raise ValueError("Please set the HUBSPOT_ACCESS_TOKEN environment variable.")

OBJECTS = {
    "carts": "crm/v3/objects/carts",
    "companies": "crm/v3/objects/companies",
    "contacts": "crm/v3/objects/contacts",
    "deals": "crm/v3/objects/deals",
    "discounts": "crm/v3/objects/discounts",
    "fees": "crm/v3/objects/fees",
    "goal_targets": "crm/v3/objects/goal_targets",
    "invoices": "crm/v3/objects/invoices",
    "line_items": "crm/v3/objects/line_items",
    # disabled/problematic by default (uncomment if you know you have permissions)
    # "orders": "crm/v3/objects/orders",
    # "partner_clients": "crm/v3/objects/partner_clients",
    # "payments": "crm/v3/objects/commerce_payments",
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
    # "feedback_submissions": "crm/v3/objects/feedback_submissions",
    "postal_mail": "crm/v3/objects/postal_mail",
}

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}


# =========================
# Utilities
# =========================
def make_session_with_retries(total_retries=SESSION_TOTAL_RETRIES, backoff=SESSION_BACKOFF_FACTOR):
    """Return a requests.Session configured with retries/backoff for idempotent errors."""
    session = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


# =========================
# ClickHouse helpers (per-thread client)
# =========================
def get_clickhouse_client():
    """Create and return a ClickHouse client instance (one per thread)."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        secure=True,
    )


def drop_and_create_table(client, table_name, df):
    """Drop and create ClickHouse table based on DataFrame schema using given client."""
    client.command(f"DROP TABLE IF EXISTS `{table_name}`")

    columns = []
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            col_type = "Int64"
        elif pd.api.types.is_float_dtype(dtype):
            col_type = "Float64"
        else:
            col_type = "String"
        # sanitize column name as a basic precaution
        safe_col = col.replace("`", "")
        columns.append(f"`{safe_col}` {col_type}")

    columns_sql = ", ".join(columns) if columns else "`_dummy` String"
    create_sql = f"""
    CREATE TABLE `{table_name}` (
        {columns_sql}
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    """
    client.command(create_sql)


def insert_into_clickhouse(client, table_name, df):
    """Insert DataFrame data into ClickHouse using given client."""
    # Normalize string columns to avoid None issues
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].fillna("").astype(str)
    # For numeric columns keep fillna(0) as needed
    df = df.where(pd.notnull(df), None)
    client.insert_df(table_name, df)


# =========================
# HubSpot fetch functions
# =========================
def fetch_properties(session, object_name):
    """Fetch all property names for an object. Returns list of names or empty list on 403/400."""
    url = f"https://api.hubapi.com/crm/v3/properties/{object_name}"
    params = {"archived": "false"}
    try:
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        names = [p["name"] for p in data.get("results", [])]
        print(f"🛠 {object_name}: {len(names)} properties fetched")
        return names
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        if code in (403, 401):
            print(f"⚠️ {object_name}: Permission denied (status {code}). Skipping properties.")
            return []
        if code == 400:
            print(f"⚠️ {object_name}: Bad request fetching properties (400). Skipping.")
            return []
        raise
    except Exception as e:
        print(f"❌ {object_name}: Error fetching properties: {e}")
        return []


def fetch_object_data_with_chunked_properties(session, object_name, endpoint, all_properties):
    """
    Fetch records for the object by requesting properties in chunks and merging properties per record id.
    Returns list of merged records (each as dict with 'id' and 'properties').
    """
    if not all_properties:
        return []

    # Map id -> {'id': id, 'properties': {...}}
    records_map = {}

    # We'll iterate property chunks; for each chunk paginate through all objects
    prop_chunks = list(chunk_list(all_properties, PROPERTIES_CHUNK_SIZE))
    for chunk_index, prop_chunk in enumerate(prop_chunks, start=1):
        params = {
            "limit": PAGE_LIMIT,
            "archived": "false",
            "properties": ",".join(prop_chunk)
        }
        page_count = 0
        url = f"https://api.hubapi.com/{endpoint}"

        # Paginate for this property chunk
        while True:
            try:
                resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
            except requests.exceptions.HTTPError as e:
                code = getattr(e.response, "status_code", None)
                if code in (403, 401):
                    print(f"⚠️ {object_name}: Permission denied when fetching data (status {code}). Skipping object.")
                    return []
                # for transient hubspot issues, re-raise to let the caller handle/retry if desired
                print(f"❌ {object_name}: HTTP error fetching data (chunk {chunk_index}): {e}")
                raise
            except Exception as e:
                print(f"❌ {object_name}: Network/other error fetching data (chunk {chunk_index}): {e}")
                raise

            results = data.get("results", [])
            page_count += 1
            print(f"📄 {object_name}: Chunk {chunk_index} - Batch {page_count} - {len(results)} records fetched")

            # Merge results into records_map
            for rec in results:
                rec_id = rec.get("id") or rec.get("objectId") or rec.get("object_id")
                if rec_id is None:
                    # fallback: some endpoints return 'objectId' or other forms; ensure we have an id
                    continue
                if rec_id not in records_map:
                    records_map[rec_id] = {"id": rec_id, "properties": {}}
                # rec['properties'] contains only the properties requested in this chunk
                properties = rec.get("properties", {})
                # merge (later chunks will add more keys)
                records_map[rec_id]["properties"].update(properties)

            # paging
            paging_next = data.get("paging", {}).get("next", {})
            if "after" in paging_next:
                params["after"] = paging_next["after"]
                # small sleep to be polite / avoid rate limits when iterating many chunks/pages
                time.sleep(0.05)
            else:
                # finished this chunk
                break

        # After finishing a chunk, remove 'after' so next chunk starts from first page again
        if "after" in params:
            params.pop("after", None)

    merged = list(records_map.values())
    print(f"✅ {object_name}: {len(merged)} total unique records fetched (merged across {len(prop_chunks)} property chunks)")
    return merged


# =========================
# Main per-object processing
# =========================
def process_object(obj, endpoint):
    """
    This runs in a worker thread:
     - creates a session and clickhouse client
     - fetches properties (graceful on 403)
     - fetches data chunking properties
     - saves CSV and loads into ClickHouse
    """
    print(f"▶️ Start processing: {obj}")
    session = make_session_with_retries()
    client = get_clickhouse_client()
    try:
        props = fetch_properties(session, obj)
        if not props:
            print(f"⚠️ {obj}: No properties available or access denied. Skipping object.")
            return

        # fetch and merge records across property chunks
        records = fetch_object_data_with_chunked_properties(session, obj, endpoint, props)
        if not records:
            print(f"⚠️ {obj}: No records found.")
            return

        # Normalize JSON -> flat dataframe. We'll include id and nested properties as columns.
        # Each record in 'records' has structure {'id': id, 'properties': {...}}
        flat_rows = []
        for rec in records:
            row = {"id": rec["id"]}
            # properties values are nested; include them as top-level keys
            props_dict = rec.get("properties", {}) or {}
            # properties may have values as objects, keep as JSON-string if not basic
            for k, v in props_dict.items():
                # HubSpot sometimes returns dict with 'value' or direct value
                if isinstance(v, dict) and "value" in v and len(v) == 1:
                    row[k] = v.get("value")
                else:
                    # if it's a complex object, JSON-dump
                    if isinstance(v, (dict, list)):
                        row[k] = json.dumps(v, ensure_ascii=False)
                    else:
                        row[k] = v
            flat_rows.append(row)

        df = pd.DataFrame(flat_rows)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(OUTPUT_DIR, f"{obj}.csv")
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"💾 Saved: {output_path}")

        # Load into ClickHouse
        # create table only if df has columns
        if df.empty:
            print(f"⚠️ {obj}: DataFrame empty after normalization. Skipping DB load.")
            return

        drop_and_create_table(client, obj, df)
        insert_into_clickhouse(client, obj, df)
        print(f"📦 Loaded into ClickHouse table: {obj}")

    except Exception as e:
        print(f"❌ Error processing {obj}: {e}")
    finally:
        try:
            client.close()
        except Exception:
            pass
        try:
            session.close()
        except Exception:
            pass
        print(f"◀️ Finished processing: {obj}")


# =========================
# Entry point
# =========================
def main():
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_object, obj, endpoint): obj for obj, endpoint in OBJECTS.items()}
        for future in as_completed(futures):
            obj = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Unhandled error for {obj}: {e}")

    print("🎯 All objects processed (done).")


if __name__ == "__main__":
    main()