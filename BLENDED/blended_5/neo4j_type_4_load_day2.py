import os
import csv
from dotenv import load_dotenv
from neo4j import GraphDatabase
from datetime import date

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4jratatui")

DAY2_FILE = os.getenv('DAY_CHANGE')  # Replace with your file name
TODAY = str(date.today())

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def apply_scd_type4():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    with driver.session() as session:
        for row in rows:
            customer_id = int(row['customer_id'])
            result = session.run("""
            MATCH (m:CustomerSCD4Main {customer_id: $customer_id})-[:HAS_CURRENT]->(c:CustomerSCD4Current)
            RETURN c
            """, customer_id=customer_id)

            record = result.single()

            if not record:
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
                customer_id=customer_id,
                full_name=row['full_name'],
                email=row['email'],
                phone=row['phone'],
                city=row['city'],
                address=row['address'],
                status=row['status'],
                sign_up_date=row['sign_up_date'],
                age=int(row['age']),
                subscription_type=row['subscription_type'])

                print(f"New customer_id={customer_id} added.")
            else:
                changes = False
                fields = ['email', 'phone', 'city', 'address', 'status', 'subscription_type']
                updates = {field: row[field] for field in fields if record['c'][field] != row[field]}
                
                if updates:
                    session.run("""
                    MATCH (m:CustomerSCD4Main {customer_id: $customer_id})-[:HAS_CURRENT]->(c:CustomerSCD4Current)
                    WITH m, c
                    CREATE (h:CustomerSCD4History {
                        full_name: c.full_name,
                        email: c.email,
                        phone: c.phone,
                        city: c.city,
                        address: c.address,
                        status: c.status,
                        sign_up_date: c.sign_up_date,
                        age: c.age,
                        subscription_type: c.subscription_type,
                        valid_from: c.sign_up_date,
                        valid_to: date($today)
                    })
                    MERGE (m)-[:HAS_HISTORY]->(h)

                    SET c += $updates,
                        c.sign_up_date = c.sign_up_date,
                        c.valid_to = NULL
                    """, customer_id=customer_id, updates=updates, today=TODAY)
                    
                    print(f"Customer_id={customer_id} updated with changes: {updates.keys()}")

if __name__ == "__main__":
    apply_scd_type4()
    print("Day 2 changes applied using SCD Type 4 for Neo4j.")
