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
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DAY1_FILE = os.getenv('DAY_INIT')  # Replace with your file name
TODAY = date.today()

connection = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    dbname=POSTGRES_DB
)

def create_tables():
    create_current_sql = """
    CREATE TABLE IF NOT EXISTS dim_customers_scd4_current (
        customer_id INT PRIMARY KEY,
        full_name TEXT,
        email TEXT,
        phone TEXT,
        city TEXT,
        address TEXT,
        status TEXT,
        sign_up_date DATE,
        age INT,
        subscription_type TEXT
    );
    """

    create_history_sql = """
    CREATE TABLE IF NOT EXISTS dim_customers_scd4_history (
        customer_id INT,
        full_name TEXT,
        email TEXT,
        phone TEXT,
        city TEXT,
        address TEXT,
        status TEXT,
        sign_up_date DATE,
        age INT,
        subscription_type TEXT,
        valid_from DATE,
        valid_to DATE
    );
    """

    with connection.cursor() as cur:
        cur.execute(create_current_sql)
        cur.execute(create_history_sql)
    connection.commit()

def load_day1_data():
    with open(DAY1_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    insert_current_sql = """
    INSERT INTO dim_customers_scd4_current (customer_id, full_name, email, phone, city, address, status, sign_up_date, age, subscription_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO NOTHING;
    """

    insert_history_sql = """
    INSERT INTO dim_customers_scd4_history (customer_id, full_name, email, phone, city, address, status, sign_up_date, age, subscription_type, valid_from, valid_to)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL);
    """

    with connection.cursor() as cur:
        for row in rows:
            cur.execute(insert_current_sql, (
                row['customer_id'],
                row['full_name'],
                row['email'],
                row['phone'],
                row['city'],
                row['address'],
                row['status'],
                row['sign_up_date'],
                row['age'],
                row['subscription_type']
            ))
            cur.execute(insert_history_sql, (
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
                TODAY
            ))
    connection.commit()

if __name__ == "__main__":
    create_tables()
    load_day1_data()
    print("Day 1 data (SCD Type 4) loaded into dim_customers_scd4_current and dim_customers_scd4_history.")
