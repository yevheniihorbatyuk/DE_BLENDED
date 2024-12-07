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

DAY2_FILE = os.getenv('DAY_CHANGE')  # Replace with your input file
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
    SELECT customer_id, full_name, email, phone, city, address, status, sign_up_date, age, subscription_type
    FROM dim_customers_scd4_current
    WHERE customer_id = %s
    """

    insert_history_sql = """
    INSERT INTO dim_customers_scd4_history (customer_id, full_name, email, phone, city, address, status, sign_up_date, age, subscription_type, valid_from, valid_to)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    update_current_sql = """
    UPDATE dim_customers_scd4_current
    SET full_name = %s,
        email = %s,
        phone = %s,
        city = %s,
        address = %s,
        status = %s,
        age = %s,
        subscription_type = %s
    WHERE customer_id = %s
    """

    insert_new_sql = """
    INSERT INTO dim_customers_scd4_current (customer_id, full_name, email, phone, city, address, status, sign_up_date, age, subscription_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

            # Fetch the current record
            cur.execute(select_current_sql, (customer_id,))
            current_record = cur.fetchone()

            if not current_record:
                # Insert new customer into current table
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
                    subscription_type_new
                ))
                print(f"New customer_id={customer_id} inserted into current (SCD4).")
            else:
                (cid, full_name_old, email_old, phone_old, city_old, address_old, status_old,
                 sign_up_date_old, age_old, subscription_type_old) = current_record

                # Check for changes in relevant fields
                changes = (email_new != email_old or phone_new != phone_old or city_new != city_old or
                           address_new != address_old or status_new != status_old or age_new != age_old or
                           subscription_type_new != subscription_type_old)

                if changes:
                    # Archive the current record into history
                    cur.execute(insert_history_sql, (
                        cid,
                        full_name_old,
                        email_old,
                        phone_old,
                        city_old,
                        address_old,
                        status_old,
                        sign_up_date_old,
                        age_old,
                        subscription_type_old,
                        sign_up_date_old,
                        TODAY
                    ))

                    # Update current table with new data
                    cur.execute(update_current_sql, (
                        full_name_new,
                        email_new,
                        phone_new,
                        city_new,
                        address_new,
                        status_new,
                        age_new,
                        subscription_type_new,
                        customer_id
                    ))
                    print(f"SCD Type 4 Update for customer_id={customer_id}: historical record added, current updated.")
                else:
                    print(f"No changes for customer_id={customer_id}, no SCD4 update needed.")

    connection.commit()

if __name__ == "__main__":
    apply_scd_type4()
    print("Day 2 changes applied using SCD Type 4.")
