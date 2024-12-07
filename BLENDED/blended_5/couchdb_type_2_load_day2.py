import os
import csv
import requests
from dotenv import load_dotenv
from datetime import date

load_dotenv()

COUCHDB_URL = os.getenv("COUCHDB_URL", "http://127.0.0.1:5984")
COUCHDB_USER = os.getenv("COUCHDB_USER", "mandarin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "mandarin223315")
DB_NAME = "dim_customers_scd2_couch"
DAY2_FILE = os.getenv('DAY_CHANGE')  # Replace with your file name
TODAY = str(date.today())
AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)

def find_current_doc(customer_id):
    query = {
        "selector": {
            "customer_id": customer_id,
            "is_current": True
        },
        "limit": 1
    }
    r = requests.post(f"{COUCHDB_URL}/{DB_NAME}/_find", auth=AUTH, json=query)
    if r.status_code == 200:
        data = r.json()
        docs = data.get("docs", [])
        if docs:
            return docs[0]
    return None

def update_doc(doc):
    doc_id = doc["_id"]
    r = requests.put(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}", auth=AUTH, json=doc)
    return r

def insert_new_doc(customer_data):
    new_doc = {
        "customer_id": int(customer_data['customer_id']),
        "full_name": customer_data['full_name'],
        "email": customer_data['email'],
        "phone": customer_data['phone'],
        "city": customer_data['city'],
        "address": customer_data['address'],
        "status": customer_data['status'],
        "sign_up_date": customer_data['sign_up_date'],
        "age": int(customer_data['age']),
        "subscription_type": customer_data['subscription_type'],
        "valid_from": TODAY,
        "valid_to": None,
        "is_current": True
    }
    new_id = f"{customer_data['customer_id']}:{TODAY}"
    return requests.put(f"{COUCHDB_URL}/{DB_NAME}/{new_id}", auth=AUTH, json=new_doc)

def apply_scd_type2():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    for row in day2_rows:
        customer_id = int(row['customer_id'])
        current_doc = find_current_doc(customer_id)

        if current_doc is None:
            resp = insert_new_doc(row)
            print(f"{'Success' if resp.status_code in (200, 201) else 'Error'} inserting new customer {customer_id}")
            continue

        fields_to_check = ['email', 'phone', 'city', 'address', 'status', 'subscription_type']
        changes = any(current_doc.get(field) != row[field] for field in fields_to_check)

        if changes:
            current_doc['valid_to'] = TODAY
            current_doc['is_current'] = False
            
            update_resp = update_doc(current_doc)
            if update_resp.status_code in (200, 201):
                new_resp = insert_new_doc(row)
                if new_resp.status_code in (200, 201):
                    print(f"Updated customer {customer_id}")
                else:
                    print(f"Error inserting new doc for {customer_id}")
            else:
                print(f"Error updating old doc for {customer_id}")
        else:
            print(f"No changes for customer {customer_id}")

if __name__ == "__main__":
    apply_scd_type2()
    print("Day 2 changes applied using SCD Type 2")
