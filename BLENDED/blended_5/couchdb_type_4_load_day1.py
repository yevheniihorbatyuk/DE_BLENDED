import os
import csv
import requests
from dotenv import load_dotenv
from datetime import date

load_dotenv()

COUCHDB_URL = os.getenv("COUCHDB_URL", "http://127.0.0.1:5984")
COUCHDB_USER = os.getenv("COUCHDB_USER", "mandarin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "mandarin223315")

CURRENT_DB = "dim_customers_scd4_couch_current"
HISTORY_DB = "dim_customers_scd4_couch_history"

DAY1_FILE = os.getenv('DAY_INIT')  # Replace with your file name
AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)
TODAY = date.today().isoformat()

def create_db_if_not_exists(db_name):
    r = requests.get(f"{COUCHDB_URL}/{db_name}", auth=AUTH)
    if r.status_code == 404:
        cr = requests.put(f"{COUCHDB_URL}/{db_name}", auth=AUTH)
        if cr.status_code in (200, 201):
            print(f"Database '{db_name}' created.")
        else:
            print(f"Error creating db '{db_name}': {cr.text}")
    elif r.status_code == 200:
        print(f"Database '{db_name}' already exists.")
    else:
        print(f"Error checking db '{db_name}': {r.text}")

def load_day1_data():
    with open(DAY1_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        doc_id = str(row['customer_id'])

        # Prepare the document for the current database
        current_doc = {
            "customer_id": int(row['customer_id']),
            "full_name": row['full_name'],
            "email": row['email'],
            "phone": row['phone'],
            "city": row['city'],
            "address": row['address'],
            "status": row['status'],
            "sign_up_date": row['sign_up_date'],
            "age": int(row['age']),
            "subscription_type": row['subscription_type']
        }

        # Prepare the document for the history database
        history_doc = {
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
            "valid_to": None
        }

        # Insert into current database
        r_current = requests.put(f"{COUCHDB_URL}/{CURRENT_DB}/{doc_id}", auth=AUTH, json=current_doc)
        if r_current.status_code in (200, 201):
            print(f"Inserted current doc for customer_id={row['customer_id']}")
        else:
            print(f"Error inserting current doc for customer_id={row['customer_id']}: {r_current.text}")

        # Insert into history database
        r_history = requests.post(f"{COUCHDB_URL}/{HISTORY_DB}", auth=AUTH, json=history_doc)
        if r_history.status_code in (200, 201):
            print(f"Inserted history doc for customer_id={row['customer_id']}")
        else:
            print(f"Error inserting history doc for customer_id={row['customer_id']}: {r_history.text}")

if __name__ == "__main__":
    create_db_if_not_exists(CURRENT_DB)
    create_db_if_not_exists(HISTORY_DB)
    load_day1_data()
    print("Day 1 data (SCD Type 4 for CouchDB) loaded into current and history DBs.")
