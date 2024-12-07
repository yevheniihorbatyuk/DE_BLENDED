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

DAY2_FILE = os.getenv('DAY_CHANGE')  # Update this to your input file name
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
    SELECT customer_id, full_name, email, previous_email, phone, city, previous_city, address, 
           status, sign_up_date, age, subscription_type, last_update_date
    FROM dim_customers_scd3
    WHERE customer_id = %s
    """

    update_sql = """
    UPDATE dim_customers_scd3
    SET email = %s,
        previous_email = %s,
        phone = %s,
        city = %s,
        previous_city = %s,
        address = %s,
        status = %s,
        age = %s,
        subscription_type = %s,
        last_update_date = %s
    WHERE customer_id = %s
    """

    insert_new_sql = """
    INSERT INTO dim_customers_scd3 (customer_id, full_name, email, previous_email, phone, city, previous_city, address, 
                                     status, sign_up_date, age, subscription_type, last_update_date)
    VALUES (%s, %s, %s, NULL, %s, %s, NULL, %s, %s, %s, %s, %s, %s)
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
                # Insert new customer if no history exists
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
                (cid, full_name_old, email_old, prev_email_old, phone_old, city_old, prev_city_old, 
                 address_old, status_old, sign_up_date_old, age_old, subscription_type_old, last_update_old) = current_record

                # Check changes in relevant fields
                email_changed = email_old != email_new
                city_changed = city_old != city_new
                phone_changed = phone_old != phone_new
                address_changed = address_old != address_new
                status_changed = status_old != status_new
                age_changed = age_old != age_new
                subscription_type_changed = subscription_type_old != subscription_type_new

                if any([email_changed, city_changed, phone_changed, address_changed, 
                        status_changed, age_changed, subscription_type_changed]):
                    # Prepare updated data
                    new_previous_email = email_old if email_changed else prev_email_old
                    new_previous_city = city_old if city_changed else prev_city_old

                    # Update the record
                    cur.execute(update_sql, (
                        email_new,
                        new_previous_email,
                        phone_new,
                        city_new,
                        new_previous_city,
                        address_new,
                        status_new,
                        age_new,
                        subscription_type_new,
                        TODAY,
                        customer_id
                    ))
                    print(f"SCD Type 3 Update for customer_id={customer_id}")
                else:
                    print(f"No changes for customer_id={customer_id}, no update needed.")

    connection.commit()


if __name__ == "__main__":
    apply_scd_type3()
    print("Changes applied using SCD Type 3.")
