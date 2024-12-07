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

DAY2_FILE = os.getenv('DAY_CHANGE')  # Update this to your input file
AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)
TODAY = str(date.today())

def get_doc(db_name, doc_id):
    r = requests.get(f"{COUCHDB_URL}/{db_name}/{doc_id}", auth=AUTH)
    if r.status_code == 200:
        return r.json()
    elif r.status_code == 404:
        return None
    else:
        print(f"Error getting doc {doc_id} from {db_name}: {r.text}")
        return None

def update_doc(db_name, doc_id, doc):
    r = requests.put(f"{COUCHDB_URL}/{db_name}/{doc_id}", auth=AUTH, json=doc)
    return r

def insert_history_record(customer_id, full_name, email, phone, city, address, status, sign_up_date, age, subscription_type, valid_from, valid_to):
    hist_id = f"{customer_id}:{valid_to}"
    doc = {
        "customer_id": customer_id,
        "full_name": full_name,
        "email": email,
        "phone": phone,
        "city": city,
        "address": address,
        "status": status,
        "sign_up_date": sign_up_date,
        "age": age,
        "subscription_type": subscription_type,
        "valid_from": valid_from,
        "valid_to": valid_to
    }
    r = requests.put(f"{COUCHDB_URL}/{HISTORY_DB}/{hist_id}", auth=AUTH, json=doc)
    return r

def apply_scd_type4():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    for row in day2_rows:
        customer_id = int(row['customer_id'])
        doc_id = str(customer_id)
        full_name_new = row['full_name']
        email_new = row['email']
        phone_new = row['phone']
        city_new = row['city']
        address_new = row['address']
        status_new = row['status']
        sign_up_date_new = row['sign_up_date']
        age_new = int(row['age'])
        subscription_type_new = row['subscription_type']

        current_doc = get_doc(CURRENT_DB, doc_id)
        if current_doc is None:
            # Insert as a new customer into current database
            new_doc = {
                "customer_id": customer_id,
                "full_name": full_name_new,
                "email": email_new,
                "phone": phone_new,
                "city": city_new,
                "address": address_new,
                "status": status_new,
                "sign_up_date": sign_up_date_new,
                "age": age_new,
                "subscription_type": subscription_type_new
            }
            resp = update_doc(CURRENT_DB, doc_id, new_doc)
            if resp.status_code in (200, 201):
                print(f"Inserted new customer_id={customer_id} into current (SCD4).")
            else:
                print(f"Error inserting new customer_id={customer_id}: {resp.text}")
        else:
            # Check for changes
            changes = (
                current_doc.get('email') != email_new or
                current_doc.get('phone') != phone_new or
                current_doc.get('city') != city_new or
                current_doc.get('address') != address_new or
                current_doc.get('status') != status_new or
                current_doc.get('age') != age_new or
                current_doc.get('subscription_type') != subscription_type_new
            )

            if changes:
                # Archive the current record in history database
                hist_resp = insert_history_record(
                    customer_id=customer_id,
                    full_name=current_doc['full_name'],
                    email=current_doc['email'],
                    phone=current_doc['phone'],
                    city=current_doc['city'],
                    address=current_doc['address'],
                    status=current_doc['status'],
                    sign_up_date=current_doc['sign_up_date'],
                    age=current_doc['age'],
                    subscription_type=current_doc['subscription_type'],
                    valid_from=current_doc['sign_up_date'],
                    valid_to=TODAY
                )

                if hist_resp.status_code in (200, 201):
                    # Update the current database with new data
                    current_doc.update({
                        "full_name": full_name_new,
                        "email": email_new,
                        "phone": phone_new,
                        "city": city_new,
                        "address": address_new,
                        "status": status_new,
                        "age": age_new,
                        "subscription_type": subscription_type_new
                    })
                    resp = update_doc(CURRENT_DB, doc_id, current_doc)
                    if resp.status_code in (200, 201):
                        print(f"SCD Type 4 Update for customer_id={customer_id}: old record moved to history, current updated.")
                    else:
                        print(f"Error updating current doc for customer_id={customer_id}: {resp.text}")
                else:
                    print(f"Error inserting history doc for customer_id={customer_id}: {hist_resp.text}")
            else:
                print(f"No changes for customer_id={customer_id}, no SCD4 update needed.")

if __name__ == "__main__":
    apply_scd_type4()
    print("Day 2 changes applied using SCD Type 4 for CouchDB.")
