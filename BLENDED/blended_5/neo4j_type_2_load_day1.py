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
        # Для SCD Type 2 корисно мати унікальний індекс на customer_id для CustomerSCD2Main
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:CustomerSCD2Main) REQUIRE c.customer_id IS UNIQUE;")

        for row in rows:
            customer_id = int(row['customer_id'])
            full_name = row['full_name']
            email = row['email']
            city = row['city']
            sign_up_date = row['sign_up_date']

            session.run("""
            MERGE (m:CustomerSCD2Main {customer_id: $customer_id})
            ON CREATE SET m.created_at = datetime()
            
            CREATE (v:CustomerSCD2Version {
                full_name: $full_name,
                email: $email,
                city: $city,
                sign_up_date: date($sign_up_date),
                valid_from: date($sign_up_date),
                valid_to: NULL,
                is_current: true
            })
            MERGE (m)-[:HAS_VERSION]->(v)
            """, 
            customer_id=customer_id, full_name=full_name, email=email, city=city, sign_up_date=sign_up_date)
    
    print("Day 1 data (SCD Type 2 for Neo4j) loaded.")

if __name__ == "__main__":
    load_day1_data()
