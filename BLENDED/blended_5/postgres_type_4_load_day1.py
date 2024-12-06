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

DAY1_FILE = "BLENDED/blended_5/data/day1_customers.csv"

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
        city TEXT,
        sign_up_date DATE
    );
    """

    create_history_sql = """
    CREATE TABLE IF NOT EXISTS dim_customers_scd4_history (
        customer_id INT,
        full_name TEXT,
        email TEXT,
        city TEXT,
        sign_up_date DATE,
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

    insert_sql = """
    INSERT INTO dim_customers_scd4_current (customer_id, full_name, email, city, sign_up_date)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO NOTHING;
    """
    with connection.cursor() as cur:
        for row in rows:
            cur.execute(insert_sql, (
                row['customer_id'],
                row['full_name'],
                row['email'],
                row['city'],
                row['sign_up_date']
            ))
    connection.commit()

if __name__ == "__main__":
    create_tables()
    load_day1_data()
    print("Day 1 data (SCD Type 4) loaded into dim_customers_scd4_current.")