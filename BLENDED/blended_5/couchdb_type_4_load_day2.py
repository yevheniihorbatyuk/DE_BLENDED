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

DAY2_FILE = "BLENDED/blended_5/data/day2_changes.csv"
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

def insert_history_record(customer_id, full_name, email, city, sign_up_date, valid_from, valid_to):
    # _id можемо сформувати як {customer_id}:{valid_to}, щоб був унікальний ключ для історії
    hist_id = f"{customer_id}:{valid_to}"
    doc = {
        "customer_id": customer_id,
        "full_name": full_name,
        "email": email,
        "city": city,
        "sign_up_date": sign_up_date,
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
        city_new = row['city']
        sign_up_date_new = row['sign_up_date']  # sign_up_date зазвичай не змінюється

        current_doc = get_doc(CURRENT_DB, doc_id)
        if current_doc is None:
            # Новий клієнт - просто додаємо у current
            new_doc = {
                "customer_id": customer_id,
                "full_name": full_name_new,
                "email": email_new,
                "city": city_new,
                "sign_up_date": sign_up_date_new
            }
            resp = update_doc(CURRENT_DB, doc_id, new_doc)
            if resp.status_code in (200,201):
                print(f"Inserted new customer_id={customer_id} into current (SCD4).")
            else:
                print(f"Error inserting new customer_id={customer_id}: {resp.text}")
        else:
            # Перевіряємо зміни
            email_old = current_doc['email']
            city_old = current_doc['city']
            changes = (email_old != email_new) or (city_old != city_new)

            if changes:
                # Перенесемо старий запис у history
                sign_up_date_old = current_doc['sign_up_date']
                # valid_from = sign_up_date_old, valid_to = TODAY
                hist_resp = insert_history_record(
                    customer_id=customer_id,
                    full_name=current_doc['full_name'],
                    email=email_old,
                    city=city_old,
                    sign_up_date=sign_up_date_old,
                    valid_from=sign_up_date_old,
                    valid_to=TODAY
                )

                if hist_resp.status_code in (200,201):
                    # Оновимо current новими даними
                    current_doc['email'] = email_new
                    current_doc['city'] = city_new
                    # _id і _rev вже є в current_doc
                    resp = update_doc(CURRENT_DB, doc_id, current_doc)
                    if resp.status_code in (200,201):
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
