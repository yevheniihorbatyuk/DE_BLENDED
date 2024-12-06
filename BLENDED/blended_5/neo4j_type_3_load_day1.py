import os
import csv
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4jratatui")

DAY1_FILE = "BLENDED/blended_5/data/day1_customers.csv"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def load_day1_data():
    with open(DAY1_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    with driver.session() as session:
        # Створимо унікальний індекс на customer_id для CustomerSCD3
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:CustomerSCD3) REQUIRE c.customer_id IS UNIQUE;")

        for row in rows:
            customer_id = int(row['customer_id'])
            full_name = row['full_name']
            email = row['email']
            city = row['city']
            sign_up_date = row['sign_up_date']

            # previous_email і previous_city = NULL
            # last_update_date = sign_up_date
            session.run("""
            MERGE (c:CustomerSCD3 {customer_id: $customer_id})
            ON CREATE SET c.full_name = $full_name,
                          c.email = $email,
                          c.previous_email = NULL,
                          c.city = $city,
                          c.previous_city = NULL,
                          c.sign_up_date = date($sign_up_date),
                          c.last_update_date = date($sign_up_date)
            """,
            customer_id=customer_id, full_name=full_name, email=email, city=city, sign_up_date=sign_up_date)

    print("Day 1 data (SCD Type 3 for Neo4j) loaded.")

if __name__ == "__main__":
    load_day1_data()
