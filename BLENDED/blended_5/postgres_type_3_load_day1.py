import os
import csv
import psycopg2
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "nastradamus")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ratatui1212332211")
POSTGRES_DB = os.getenv("POSTGRES_DB", "baza")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DAY1_FILE = os.getenv('DAY_INIT')  # Replace with the correct CSV filename

connection = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    dbname=POSTGRES_DB
)

def create_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dim_customers_scd3 (
        customer_id INT PRIMARY KEY,
        full_name TEXT,
        email TEXT,
        previous_email TEXT,
        phone TEXT,
        city TEXT,
        previous_city TEXT,
        address TEXT,
        status TEXT,
        sign_up_date DATE,
        age INT,
        subscription_type TEXT,
        last_update_date DATE
    );
    """
    with connection.cursor() as cur:
        cur.execute(create_table_sql)
    connection.commit()

def load_day1_data():
    with open(DAY1_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    insert_sql = """
    INSERT INTO dim_customers_scd3 (
        customer_id, full_name, email, previous_email, phone, city, previous_city, 
        address, status, sign_up_date, age, subscription_type, last_update_date
    )
    VALUES (%s, %s, %s, NULL, %s, %s, NULL, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO NOTHING;
    """
    with connection.cursor() as cur:
        for row in rows:
            cur.execute(insert_sql, (
                row['customer_id'],
                row['full_name'],
                row['email'],
                row['phone'],
                row['city'],
                row['address'],
                row['status'],
                row['sign_up_date'],
                row['age'],
                row['subscription_type'],
                row['sign_up_date']  # Initial load: last_update_date = sign_up_date
            ))
    connection.commit()


if __name__ == "__main__":
    create_table()
    load_day1_data()
    print("Day 1 data (SCD Type 3) loaded into dim_customers_scd3.")
