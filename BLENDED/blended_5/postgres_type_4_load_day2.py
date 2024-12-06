import os
import csv
import psycopg2
from dotenv import load_dotenv
from datetime import date

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "nastradamus")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ratatui1212332211")
POSTGRES_DB = os.getenv("POSTGRES_DB", "baza")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5442")

DAY2_FILE = "BLENDED/blended_5/data/day2_changes.csv"
TODAY = date.today()

connection = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    dbname=POSTGRES_DB
)

def apply_scd_type4():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    select_current_sql = """
    SELECT customer_id, full_name, email, city, sign_up_date
    FROM dim_customers_scd4_current
    WHERE customer_id = %s
    """

    insert_history_sql = """
    INSERT INTO dim_customers_scd4_history (customer_id, full_name, email, city, sign_up_date, valid_from, valid_to)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    update_current_sql = """
    UPDATE dim_customers_scd4_current
    SET full_name = %s,
        email = %s,
        city = %s,
        sign_up_date = %s
    WHERE customer_id = %s
    """

    insert_new_sql = """
    INSERT INTO dim_customers_scd4_current (customer_id, full_name, email, city, sign_up_date)
    VALUES (%s, %s, %s, %s, %s)
    """

    with connection.cursor() as cur:
        for row in day2_rows:
            customer_id = int(row['customer_id'])
            full_name_new = row['full_name']
            email_new = row['email']
            city_new = row['city']
            sign_up_date_new = row['sign_up_date']

            # Отримуємо поточний запис
            cur.execute(select_current_sql, (customer_id,))
            current_record = cur.fetchone()

            if not current_record:
                # Новий клієнт - просто додаємо в current без історії
                cur.execute(insert_new_sql, (
                    customer_id,
                    full_name_new,
                    email_new,
                    city_new,
                    sign_up_date_new
                ))
                print(f"New customer_id={customer_id} inserted into current (SCD4).")
            else:
                cid, full_name_old, email_old, city_old, sign_up_date_old = current_record

                # Перевіряємо зміни
                changes = (email_new != email_old) or (city_new != city_old)
                if changes:
                    # Переносимо старі дані в історію
                    # valid_from = sign_up_date_old (бо з того часу діяв цей запис)
                    # valid_to = TODAY (цей день став останнім, коли діяли старі дані)
                    cur.execute(insert_history_sql, (
                        cid,
                        full_name_old,
                        email_old,
                        city_old,
                        sign_up_date_old,
                        sign_up_date_old,
                        TODAY
                    ))

                    # Оновлюємо current новими даними
                    cur.execute(update_current_sql, (
                        full_name_new,
                        email_new,
                        city_new,
                        sign_up_date_old,  # sign_up_date зазвичай не змінюється, залишається оригінальним
                        customer_id
                    ))
                    print(f"SCD Type 4 Update for customer_id={customer_id}: historical record added, current updated.")
                else:
                    print(f"No changes for customer_id={customer_id}, no SCD4 update needed.")

    connection.commit()

if __name__ == "__main__":
    apply_scd_type4()
    print("Day 2 changes applied using SCD Type 4.")
