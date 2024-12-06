import os
import csv
import requests
from dotenv import load_dotenv

load_dotenv()

COUCHDB_URL = os.getenv("COUCHDB_URL", "http://127.0.0.1:5984")
COUCHDB_USER = os.getenv("COUCHDB_USER", "mandarin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "mandarin223315")

CURRENT_DB = "dim_customers_scd4_couch_current"
HISTORY_DB = "dim_customers_scd4_couch_history"

DAY1_FILE = "BLENDED/blended_5/data/day1_customers.csv"
AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)

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
        # Для поточної бази зберігаємо просто актуальні дані, без valid_from/valid_to.
        doc = {
            "customer_id": int(row['customer_id']),
            "full_name": row['full_name'],
            "email": row['email'],
            "city": row['city'],
            "sign_up_date": row['sign_up_date']
        }

        r = requests.put(f"{COUCHDB_URL}/{CURRENT_DB}/{doc_id}", auth=AUTH, json=doc)
        if r.status_code in (200, 201):
            print(f"Inserted current doc for customer_id={row['customer_id']}")
        else:
            print(f"Error inserting doc for customer_id={row['customer_id']}: {r.text}")

if __name__ == "__main__":
    create_db_if_not_exists(CURRENT_DB)
    create_db_if_not_exists(HISTORY_DB)
    load_day1_data()
    print("Day 1 data (SCD Type 4 for CouchDB) loaded into current DB.")
