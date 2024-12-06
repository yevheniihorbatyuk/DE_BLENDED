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

DAY2_FILE = "BLENDED/blended_5/data/day2_changes.csv"
TODAY = str(date.today())  # Рядок типу "2024-12-06"

AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)

def find_current_doc(customer_id):
    # Використовуємо Mango Query для пошуку документа з is_current=true
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
    # Оновлення існуючого документу (потрібен _id і _rev)
    doc_id = doc["_id"]
    r = requests.put(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}", auth=AUTH, json=doc)
    return r

def insert_new_doc(customer_id, full_name, email, city, sign_up_date):
    new_doc = {
        "customer_id": customer_id,
        "full_name": full_name,
        "email": email,
        "city": city,
        "sign_up_date": sign_up_date,
        "valid_from": TODAY,
        "valid_to": None,
        "is_current": True
    }
    new_id = f"{customer_id}:{TODAY}"
    r = requests.put(f"{COUCHDB_URL}/{DB_NAME}/{new_id}", auth=AUTH, json=new_doc)
    return r

def apply_scd_type2():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    for row in day2_rows:
        customer_id = int(row['customer_id'])
        full_name_new = row['full_name']
        email_new = row['email']
        city_new = row['city']
        sign_up_date_new = row['sign_up_date']  # зазвичай sign_up_date не змінюємо

        current_doc = find_current_doc(customer_id)
        if current_doc is None:
            # Новий клієнт, просто вставляємо новий документ
            resp = insert_new_doc(customer_id, full_name_new, email_new, city_new, sign_up_date_new)
            if resp.status_code in (200,201):
                print(f"Inserted new customer_id={customer_id} as new doc.")
            else:
                print(f"Error inserting new customer_id={customer_id}: {resp.text}")
        else:
            # Перевіримо, чи є зміни
            email_old = current_doc['email']
            city_old = current_doc['city']
            changes = (email_old != email_new) or (city_old != city_new)

            if changes:
                # Оновлюємо старий документ: встановлюємо valid_to=TODAY-1, is_current=FALSE
                # Для спрощення візьмемо valid_to=TODAY. Означає, що до цього дня він був актуальний.
                current_doc['valid_to'] = TODAY
                current_doc['is_current'] = False
                update_resp = update_doc(current_doc)
                if update_resp.status_code in (200,201):
                    # Додаємо новий документ із новими даними
                    new_resp = insert_new_doc(customer_id, full_name_new, email_new, city_new, current_doc['sign_up_date'])
                    if new_resp.status_code in (200,201):
                        print(f"SCD Type 2 Update for customer_id={customer_id}: old doc closed, new doc inserted.")
                    else:
                        print(f"Error inserting new doc for customer_id={customer_id}: {new_resp.text}")
                else:
                    print(f"Error updating old doc for customer_id={customer_id}: {update_resp.text}")
            else:
                print(f"No changes for customer_id={customer_id}, no SCD2 update needed.")


if __name__ == "__main__":
    apply_scd_type2()
    print("Day 2 changes applied using SCD Type 2 for CouchDB.")
