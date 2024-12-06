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

def create_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id INT,
        full_name TEXT,
        email TEXT,
        city TEXT,
        sign_up_date DATE,
        valid_from DATE,
        valid_to DATE,
        is_current BOOLEAN,
        PRIMARY KEY (customer_id, valid_from)
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
    INSERT INTO dim_customers (customer_id, full_name, email, city, sign_up_date, valid_from, valid_to, is_current)
    VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE)
    ON CONFLICT (customer_id, valid_from) DO NOTHING;
    """
    with connection.cursor() as cur:
        for row in rows:
            # valid_from = sign_up_date (з першого дня)
            cur.execute(insert_sql, (
                row['customer_id'],
                row['full_name'],
                row['email'],
                row['city'],
                row['sign_up_date'],
                row['sign_up_date']  # Для SCD Type 2, перший запис valid_from = sign_up_date
            ))
    connection.commit()


if __name__ == "__main__":
    create_table()
    load_day1_data()
    print("Day 1 data loaded into dim_customers.")
