import os
import csv
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4jratatui")

DAY1_FILE = os.getenv('DAY_INIT')  # Replace with your file name

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def load_day1_data():
    with open(DAY1_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    with driver.session() as session:
        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS FOR (c:CustomerSCD4Main) REQUIRE c.customer_id IS UNIQUE;
        """)

        for row in rows:
            session.run("""
            MERGE (m:CustomerSCD4Main {customer_id: $customer_id})
            ON CREATE SET m.created_at = datetime()

            CREATE (c:CustomerSCD4Current {
                full_name: $full_name,
                email: $email,
                phone: $phone,
                city: $city,
                address: $address,
                status: $status,
                sign_up_date: date($sign_up_date),
                age: $age,
                subscription_type: $subscription_type
            })

            MERGE (m)-[:HAS_CURRENT]->(c)
            """,
            customer_id=int(row['customer_id']),
            full_name=row['full_name'],
            email=row['email'],
            phone=row['phone'],
            city=row['city'],
            address=row['address'],
            status=row['status'],
            sign_up_date=row['sign_up_date'],
            age=int(row['age']),
            subscription_type=row['subscription_type'])

    print("Day 1 data (SCD Type 4 for Neo4j) loaded.")

if __name__ == "__main__":
    load_day1_data()
