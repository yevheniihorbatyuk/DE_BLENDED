import os
import csv
import requests
from dotenv import load_dotenv

load_dotenv()

COUCHDB_URL = os.getenv("COUCHDB_URL", "http://127.0.0.1:5984")
COUCHDB_USER = os.getenv("COUCHDB_USER", "mandarin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "mandarin223315")
DB_NAME = "dim_customers_scd3_couch"
DAY1_FILE = os.getenv('DAY_INIT')  # Replace with your input file
AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)

def create_db_if_not_exists():
    r = requests.get(f"{COUCHDB_URL}/{DB_NAME}", auth=AUTH)
    if r.status_code == 404:
        cr = requests.put(f"{COUCHDB_URL}/{DB_NAME}", auth=AUTH)
        print("Database created" if cr.status_code in (200, 201) else f"Error: {cr.text}")
    else:
        print("Database exists" if r.status_code == 200 else f"Error: {r.text}")

def load_day1_data():
    with open(DAY1_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        doc = {
            "customer_id": int(row['customer_id']),
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
            "sign_up_date": row['sign_up_date'],
            "age": int(row['age']),
            "subscription_type": row['subscription_type'],
            "previous_subscription_type": None,
            "last_update_date": row['sign_up_date']
        }

        # Insert into CouchDB
        r = requests.put(f"{COUCHDB_URL}/{DB_NAME}/{row['customer_id']}", auth=AUTH, json=doc)
        print(f"{'Success' if r.status_code in (200, 201) else 'Error'} for ID {row['customer_id']}")

if __name__ == "__main__":
    create_db_if_not_exists()
    load_day1_data()
    print("Day 1 data (SCD Type 3 for CouchDB) loaded.")
