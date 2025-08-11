import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor

fake = Faker()
np.random.seed(42)

OUTPUT_DIR = "sample_hubspot_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# HubSpot CRM objects mapping
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
    "tickets": "crm/v3/objects/tickets",
    "calls": "crm/v3/objects/calls",
    "emails": "crm/v3/objects/emails",
    "meetings": "crm/v3/objects/meetings",
    "notes": "crm/v3/objects/notes",
    "tasks": "crm/v3/objects/tasks",
    "communications": "crm/v3/objects/communications",
    "conversations": "crm/v3/objects/conversations",
    "feedback_submissions": "crm/v3/objects/feedback_submissions",
    "postal_mail": "crm/v3/objects/postal_mail",
}

NUM_ROWS = 100
start_date = pd.to_datetime("2025-01-01")
end_date = pd.to_datetime("2025-08-10")

def random_dates(start, end, n):
    start_u = start.value // 10**9
    end_u = end.value // 10**9
    return [datetime.fromtimestamp(np.random.randint(start_u, end_u)) for _ in range(n)]

def generate_object_data(object_name):
    """Generate a sample DataFrame for a given HubSpot object."""
    base_data = {
        "id": [fake.uuid4() for _ in range(NUM_ROWS)],
        "createdAt": random_dates(start_date, end_date, NUM_ROWS),
        "hs_object_id": [fake.random_int(100000, 999999) for _ in range(NUM_ROWS)]
    }

    # Add object-specific fake fields
    if object_name == "companies":
        base_data.update({
            "company_name": [fake.company() for _ in range(NUM_ROWS)],
            "industry": np.random.choice(["Tech", "Finance", "Healthcare", "Retail"], NUM_ROWS),
        })
    elif object_name == "contacts":
        base_data.update({
            "first_name": [fake.first_name() for _ in range(NUM_ROWS)],
            "last_name": [fake.last_name() for _ in range(NUM_ROWS)],
            "email": [fake.email() for _ in range(NUM_ROWS)],
        })
    elif object_name == "deals":
        base_data.update({
            "deal_name": [fake.catch_phrase() for _ in range(NUM_ROWS)],
            "amount": np.random.randint(1000, 20000, NUM_ROWS),
            "deal_stage": np.random.choice(["Prospecting", "Negotiation", "Closed Won", "Closed Lost"], NUM_ROWS),
        })
    elif object_name == "calls":
        base_data.update({
            "rep_name": [fake.name() for _ in range(NUM_ROWS)],
            "call_type": np.random.choice(["Outbound", "Inbound"], NUM_ROWS),
            "outcome": np.random.choice(["Connected", "Voicemail", "No Answer", "Busy"], NUM_ROWS),
            "duration": np.random.randint(30, 1800, NUM_ROWS),
            "phone_number": [fake.phone_number() for _ in range(NUM_ROWS)]
        })
    else:
        # Generic fields for other objects
        base_data.update({
            "name": [fake.word().capitalize() for _ in range(NUM_ROWS)],
            "description": [fake.sentence() for _ in range(NUM_ROWS)],
        })

    df = pd.DataFrame(base_data)
    file_path = f"{OUTPUT_DIR}/{object_name}.csv"
    df.to_csv(file_path, index=False)
    return f"✅ {object_name} - {len(df)} records generated"

# Run in parallel
with ThreadPoolExecutor() as executor:
    results = list(executor.map(generate_object_data, OBJECTS.keys()))

# Log output
for res in results:
    print(res)

print(f"\nAll sample CSV files generated in '{OUTPUT_DIR}' folder.")
