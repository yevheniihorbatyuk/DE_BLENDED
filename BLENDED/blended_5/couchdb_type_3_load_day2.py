import os
import csv
import requests
from dotenv import load_dotenv
from datetime import date

load_dotenv()

COUCHDB_URL = os.getenv("COUCHDB_URL", "http://127.0.0.1:5984")
COUCHDB_USER = os.getenv("COUCHDB_USER", "mandarin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "mandarin223315")
DB_NAME = "dim_customers_scd3_couch"
DAY2_FILE = os.getenv('DAY_CHANGE')  # Replace with your input file
AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)
TODAY = str(date.today())

def update_doc(doc_id, doc):
    return requests.put(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}", auth=AUTH, json=doc)

def apply_scd_type3():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    for row in day2_rows:
        customer_id = row['customer_id']
        get_resp = requests.get(f"{COUCHDB_URL}/{DB_NAME}/{customer_id}", auth=AUTH)
        
        if get_resp.status_code == 404:
            # New customer, insert record
            doc = {
                "customer_id": int(customer_id),
                "full_name": row['full_name'],
                "email": row['email'],
                "previous_email": None,
                "phone": row['phone'],
                "previous_phone": None,
                "city": row['city'],
                "previous_city": None,
                "address": row['address'],
                "previous_address": None,
                "status": row['status'],
                "previous_status": None,
                "age": int(row['age']),
                "subscription_type": row['subscription_type'],
                "previous_subscription_type": None,
                "sign_up_date": row['sign_up_date'],
                "last_update_date": TODAY
            }
            put_resp = requests.put(f"{COUCHDB_URL}/{DB_NAME}/{customer_id}", auth=AUTH, json=doc)
            print(f"New customer {customer_id} inserted: {put_resp.status_code in (200, 201)}")
            continue

        if get_resp.status_code == 200:
            # Customer exists, check for changes
            doc = get_resp.json()
            fields = {
                'email': 'previous_email',
                'phone': 'previous_phone',
                'city': 'previous_city',
                'address': 'previous_address',
                'status': 'previous_status',
                'subscription_type': 'previous_subscription_type'
            }

            changed = False
            for curr, prev in fields.items():
                if doc.get(curr) != row[curr]:
                    doc[prev] = doc.get(curr)
                    doc[curr] = row[curr]
                    changed = True

            if changed:
                doc['last_update_date'] = TODAY
                resp = update_doc(customer_id, doc)
                print(f"Updated customer {customer_id}: {resp.status_code in (200, 201)}")
            else:
                print(f"No changes for customer {customer_id}")
        else:
            print(f"Error getting customer {customer_id}: {get_resp.status_code}")

if __name__ == "__main__":
    apply_scd_type3()
    print("Day 2 changes applied using SCD Type 3 for CouchDB.")
