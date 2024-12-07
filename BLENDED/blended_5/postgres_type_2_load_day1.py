import os
import csv
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "139.162.133.152")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "customer_db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# File containing customer data
DATA_FILE = os.getenv("DAY_INIT")

# Establish database connection
connection = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    dbname=POSTGRES_DB
)

def create_table():
    """
    Create table for customer data with support for SCD Type 2.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id INT NOT NULL,
        full_name TEXT NOT NULL,
        email TEXT NOT NULL,
        phone TEXT,
        city TEXT,
        address TEXT,
        status TEXT,
        sign_up_date DATE NOT NULL,
        age INT,
        subscription_type TEXT,
        valid_from DATE NOT NULL,
        valid_to DATE,
        is_current BOOLEAN NOT NULL,
        PRIMARY KEY (customer_id, valid_from)
    );
    """
    with connection.cursor() as cur:
        cur.execute(create_table_sql)
    connection.commit()

def load_data():
    """
    Load customer data from the CSV file into the database.
    """
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    insert_sql = """
    INSERT INTO dim_customers (
        customer_id, full_name, email, phone, city, address, status,
        sign_up_date, age, subscription_type, valid_from, valid_to, is_current
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, TRUE)
    ON CONFLICT (customer_id, valid_from) DO NOTHING;
    """
    with connection.cursor() as cur:
        for row in rows:
            cur.execute(insert_sql, (
                int(row['customer_id']),
                row['full_name'],
                row['email'],
                row['phone'],
                row['city'],
                row['address'],
                row['status'],
                row['sign_up_date'],
                int(row['age']),
                row['subscription_type'],
                row['sign_up_date']  # valid_from is set to sign_up_date for initial load
            ))
    connection.commit()

if __name__ == "__main__":
    create_table()
    load_data()
    print("Customer data loaded into dim_customers.")
