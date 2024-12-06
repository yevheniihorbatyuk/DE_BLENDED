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

def apply_scd_type3():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    select_current_sql = """
    SELECT customer_id, full_name, email, previous_email, city, previous_city, sign_up_date, last_update_date
    FROM dim_customers_scd3
    WHERE customer_id = %s
    """

    update_sql = """
    UPDATE dim_customers_scd3
    SET email = %s,
        previous_email = %s,
        city = %s,
        previous_city = %s,
        last_update_date = %s
    WHERE customer_id = %s
    """

    with connection.cursor() as cur:
        for row in day2_rows:
            customer_id = int(row['customer_id'])
            full_name_new = row['full_name']
            email_new = row['email']
            city_new = row['city']
            # sign_up_date не змінюється
            sign_up_date_new = row['sign_up_date']

            # Отримуємо поточний запис
            cur.execute(select_current_sql, (customer_id,))
            current_record = cur.fetchone()

            if not current_record:
                # Новий клієнт? Для SCD Type 3 - просто вставимо як новий рядок, без історії.
                insert_new_sql = """
                INSERT INTO dim_customers_scd3 (customer_id, full_name, email, previous_email, city, previous_city, sign_up_date, last_update_date)
                VALUES (%s, %s, %s, NULL, %s, NULL, %s, %s)
                """
                cur.execute(insert_new_sql, (
                    customer_id,
                    full_name_new,
                    email_new,
                    city_new,
                    sign_up_date_new,
                    TODAY
                ))
                print(f"Inserted new customer_id={customer_id} with no previous history (SCD3).")
            else:
                (cid, full_name_old, email_old, prev_email_old, city_old, prev_city_old, sign_up_date_old, last_update_old) = current_record

                # Перевіримо зміни
                email_changed = (email_old != email_new)
                city_changed = (city_old != city_new)

                if email_changed or city_changed:
                    # Нові previous_* будуть це старі значення, якщо змінилося поле
                    # Якщо email змінився:
                    new_previous_email = prev_email_old
                    new_email = email_old
                    if email_changed:
                        # Переносимо старий email в previous_email, а новий в email
                        new_previous_email = email_old
                        new_email = email_new
                    else:
                        new_email = email_old # якщо не змінився, залишаємо як було

                    # Те ж саме для city:
                    new_previous_city = prev_city_old
                    new_city = city_old
                    if city_changed:
                        new_previous_city = city_old
                        new_city = city_new
                    else:
                        new_city = city_old

                    # Виконуємо оновлення
                    cur.execute(update_sql, (
                        new_email,
                        new_previous_email,
                        new_city,
                        new_previous_city,
                        TODAY,
                        customer_id
                    ))

                    print(f"SCD Type 3 Update for customer_id={customer_id}")
                else:
                    print(f"No changes for customer_id={customer_id}, no SCD3 update needed.")

    connection.commit()


if __name__ == "__main__":
    apply_scd_type3()
    print("Day 2 changes applied using SCD Type 3.")
