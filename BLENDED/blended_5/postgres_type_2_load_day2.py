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

DAY2_FILE = os.getenv('DAY_CHANGE')  # Update the filename
TODAY = date.today()  # Current date for processing

connection = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    dbname=POSTGRES_DB
)

def apply_scd_type2():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    # SQL Queries
    select_current_sql = """
    SELECT customer_id, full_name, email, city, sign_up_date, valid_from
    FROM dim_customers
    WHERE customer_id = %s AND is_current = TRUE
    """

    update_old_sql = """
    UPDATE dim_customers
    SET valid_to = %s, is_current = FALSE
    WHERE customer_id = %s AND is_current = TRUE
    """

    insert_new_sql = """
    INSERT INTO dim_customers (customer_id, full_name, email, phone, city, address, status, sign_up_date, age, subscription_type, valid_from, valid_to, is_current)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, TRUE)
    """

    with connection.cursor() as cur:
        for row in day2_rows:
            customer_id = int(row['customer_id'])
            full_name_new = row['full_name']
            email_new = row['email']
            phone_new = row['phone']
            city_new = row['city']
            address_new = row['address']
            status_new = row['status']
            sign_up_date_new = row['sign_up_date']
            age_new = int(row['age'])
            subscription_type_new = row['subscription_type']

            # Fetch current record
            cur.execute(select_current_sql, (customer_id,))
            current_record = cur.fetchone()

            if not current_record:
                # Insert new record for a new customer
                cur.execute(insert_new_sql, (
                    customer_id,
                    full_name_new,
                    email_new,
                    phone_new,
                    city_new,
                    address_new,
                    status_new,
                    sign_up_date_new,
                    age_new,
                    subscription_type_new,
                    TODAY
                ))
                print(f"Inserted new record for customer_id={customer_id}")
            else:
                # Check for changes
                (cid, full_name_old, email_old, city_old, sign_up_date_old, valid_from_old) = current_record
                changes = (email_old != email_new) or (city_old != city_new)

                if changes:
                    # Update old record and insert new record
                    cur.execute(update_old_sql, (TODAY, customer_id))
                    cur.execute(insert_new_sql, (
                        customer_id,
                        full_name_new,
                        email_new,
                        phone_new,
                        city_new,
                        address_new,
                        status_new,
                        sign_up_date_old,
                        age_new,
                        subscription_type_new,
                        TODAY
                    ))
                    print(f"SCD Type 2 Update for customer_id={customer_id}: New record inserted.")
                else:
                    print(f"No changes for customer_id={customer_id}, no update needed.")

    connection.commit()

if __name__ == "__main__":
    apply_scd_type2()
    print("Changes applied using SCD Type 2.")
