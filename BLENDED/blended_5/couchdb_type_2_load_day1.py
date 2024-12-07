import os
import csv
import requests
from dotenv import load_dotenv
from datetime import date

load_dotenv()

COUCHDB_URL = os.getenv("COUCHDB_URL", "http://139.162.133.152:5984")
COUCHDB_USER = os.getenv("COUCHDB_USER", "mandarin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "mandarin223315")
DB_NAME = "dim_customers_scd2_couch"

DAY1_FILE = os.getenv('DAY_INIT')  # Replace with your file name
AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)
TODAY = str(date.today())

def create_db_if_not_exists():
    r = requests.get(f"{COUCHDB_URL}/{DB_NAME}", auth=AUTH)
    if r.status_code == 404:
        cr = requests.put(f"{COUCHDB_URL}/{DB_NAME}", auth=AUTH)
        if cr.status_code in (200, 201):
            print(f"Database '{DB_NAME}' created.")
        else:
            print(f"Error creating db '{DB_NAME}': {cr.text}")
    elif r.status_code == 200:
        print(f"Database '{DB_NAME}' already exists.")
    else:
        print(f"Error checking db '{DB_NAME}': {r.text}")

def load_day1_data():
    with open(DAY1_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        doc = {
            "customer_id": int(row['customer_id']),
            "full_name": row['full_name'],
            "email": row['email'],
            "phone": row['phone'],
            "city": row['city'],
            "address": row['address'],
            "status": row['status'],
            "sign_up_date": row['sign_up_date'],
            "age": int(row['age']),
            "subscription_type": row['subscription_type'],
            "valid_from": TODAY,
            "valid_to": None,
            "is_current": True
        }

        doc_id = f"{doc['customer_id']}:{doc['valid_from']}"

        r = requests.put(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}", auth=AUTH, json=doc)
        if r.status_code in (200, 201):
            print(f"Inserted doc for customer_id={doc['customer_id']}")
        else:
            print(f"Error inserting doc for customer_id={doc['customer_id']}: {r.text}")

if __name__ == "__main__":
    create_db_if_not_exists()
    load_day1_data()
    print("Day 1 data loaded to CouchDB.")
