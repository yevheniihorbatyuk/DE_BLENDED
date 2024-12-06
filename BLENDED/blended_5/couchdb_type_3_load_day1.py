import os
import csv
import requests
from dotenv import load_dotenv

load_dotenv()

COUCHDB_URL = os.getenv("COUCHDB_URL", "http://127.0.0.1:5984")
COUCHDB_USER = os.getenv("COUCHDB_USER", "mandarin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "mandarin223315")
DB_NAME = "dim_customers_scd3_couch"

DAY1_FILE = "BLENDED/blended_5/data/day1_customers.csv"
AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)

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
        doc_id = str(row['customer_id'])
        doc = {
            "customer_id": int(row['customer_id']),
            "full_name": row['full_name'],
            "email": row['email'],
            "previous_email": None,
            "city": row['city'],
            "previous_city": None,
            "sign_up_date": row['sign_up_date'],
            "last_update_date": row['sign_up_date']  # На початку last_update_date = sign_up_date
        }

        r = requests.put(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}", auth=AUTH, json=doc)
        if r.status_code in (200, 201):
            print(f"Inserted doc for customer_id={row['customer_id']}")
        else:
            print(f"Error inserting doc for customer_id={row['customer_id']}: {r.text}")

if __name__ == "__main__":
    create_db_if_not_exists()
    load_day1_data()
    print("Day 1 data (SCD Type 3 for CouchDB) loaded.")
