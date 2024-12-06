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

DAY2_FILE = "BLENDED/blended_5/data/day2_changes.csv"
AUTH = (COUCHDB_USER, COUCHDB_PASSWORD)
TODAY = str(date.today())

def update_doc(doc_id, doc):
    # Оновлення документа в CouchDB (потрібен _id і _rev)
    r = requests.put(f"{COUCHDB_URL}/{DB_NAME}/{doc_id}", auth=AUTH, json=doc)
    return r

def apply_scd_type3():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    for row in day2_rows:
        customer_id = row['customer_id']
        # Отримуємо поточний документ за _id = customer_id
        get_resp = requests.get(f"{COUCHDB_URL}/{DB_NAME}/{customer_id}", auth=AUTH)
        if get_resp.status_code == 404:
            # Новий клієнт, створюємо новий документ без історії
            doc = {
                "customer_id": int(customer_id),
                "full_name": row['full_name'],
                "email": row['email'],
                "previous_email": None,
                "city": row['city'],
                "previous_city": None,
                "sign_up_date": row['sign_up_date'],
                "last_update_date": TODAY
            }
            put_resp = requests.put(f"{COUCHDB_URL}/{DB_NAME}/{customer_id}", auth=AUTH, json=doc)
            if put_resp.status_code in (200,201):
                print(f"Inserted new customer_id={customer_id} (SCD3).")
            else:
                print(f"Error inserting new customer_id={customer_id}: {put_resp.text}")
        elif get_resp.status_code == 200:
            doc = get_resp.json()
            # Порівнюємо email та city
            email_old = doc['email']
            city_old = doc['city']
            email_new = row['email']
            city_new = row['city']

            email_changed = (email_old != email_new)
            city_changed = (city_old != city_new)

            if email_changed or city_changed:
                # Оновлюємо previous_email та previous_city при потребі
                if email_changed:
                    doc['previous_email'] = email_old
                    doc['email'] = email_new
                # Якщо не змінився email, залишаємо previous_email як було

                if city_changed:
                    doc['previous_city'] = city_old
                    doc['city'] = city_new
                # Якщо не змінився city, залишаємо previous_city як було

                doc['last_update_date'] = TODAY

                # Оновлюємо документ
                update_resp = update_doc(customer_id, doc)
                if update_resp.status_code in (200, 201):
                    print(f"SCD Type 3 Update for customer_id={customer_id}")
                else:
                    print(f"Error updating doc for customer_id={customer_id}: {update_resp.text}")
            else:
                print(f"No changes for customer_id={customer_id}, no SCD3 update needed.")
        else:
            print(f"Error retrieving doc for customer_id={customer_id}: {get_resp.text}")

if __name__ == "__main__":
    apply_scd_type3()
    print("Day 2 changes applied using SCD Type 3 for CouchDB.")
