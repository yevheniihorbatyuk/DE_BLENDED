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

def apply_scd_type3():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    with driver.session() as session:
        for row in rows:
            customer_id = int(row['customer_id'])
            result = session.run("""
            MATCH (c:CustomerSCD3 {customer_id: $customer_id})
            RETURN c
            """, customer_id=customer_id)

            record = result.single()

            if not record:
                session.run("""
                CREATE (c:CustomerSCD3 {
                    customer_id: $customer_id,
                    full_name: $full_name,
                    email: $email,
                    previous_email: NULL,
                    phone: $phone,
                    previous_phone: NULL,
                    city: $city,
                    previous_city: NULL,
                    address: $address,
                    previous_address: NULL,
                    status: $status,
                    previous_status: NULL,
                    sign_up_date: date($sign_up_date),
                    age: $age,
                    subscription_type: $subscription_type,
                    previous_subscription_type: NULL,
                    last_update_date: date($today)
                })
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
                subscription_type=row['subscription_type'],
                today=TODAY)

                print(f"New customer_id={customer_id} added.")
            else:
                changes = False
                fields = ['email', 'phone', 'city', 'address', 'status', 'subscription_type']
                updates = {f'previous_{field}': record['c'][field] for field in fields if record['c'][field] != row[field]}
                updates.update({field: row[field] for field in fields if record['c'][field] != row[field]})
                
                if updates:
                    session.run(f"""
                    MATCH (c:CustomerSCD3 {{customer_id: $customer_id}})
                    SET {" ,".join([f'c.{key} = ${key}' for key in updates.keys()])},
                        c.last_update_date = date($today)
                    """, customer_id=customer_id, **updates, today=TODAY)
                    
                    print(f"Customer_id={customer_id} updated with changes: {updates.keys()}")

if __name__ == "__main__":
    apply_scd_type3()
    print("Day 2 changes applied using SCD Type 3 for Neo4j.")
