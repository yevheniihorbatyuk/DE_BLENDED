import os
import csv
from dotenv import load_dotenv
from neo4j import GraphDatabase
from datetime import date

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4jratatui")

DAY2_FILE = os.getenv('DAY_CHANGE')  # Replace with your input file
TODAY = str(date.today())

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def apply_scd_type2():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    with driver.session() as session:
        for row in day2_rows:
            customer_id = int(row['customer_id'])
            full_name_new = row['full_name']
            email_new = row['email']
            phone_new = row['phone']
            city_new = row['city']
            address_new = row['address']
            status_new = row['status']
            age_new = int(row['age'])
            subscription_type_new = row['subscription_type']

            result = session.run("""
            MATCH (m:CustomerSCD2Main {customer_id: $customer_id})-[:HAS_VERSION]->(v:CustomerSCD2Version {is_current:true})
            RETURN v.email AS email_old, v.phone AS phone_old, v.city AS city_old, 
                   v.address AS address_old, v.status AS status_old,
                   v.subscription_type AS subscription_type_old, v
            """, customer_id=customer_id)

            record = result.single()

            # Handle missing fields gracefully
            if record is None:
                record = {}  # No existing version

            email_old = record.get('email_old', None)
            phone_old = record.get('phone_old', None)
            city_old = record.get('city_old', None)
            address_old = record.get('address_old', None)
            status_old = record.get('status_old', None)
            subscription_type_old = record.get('subscription_type_old', None)

            if record == {}:  # Insert new version if no record exists
                session.run("""
                MERGE (m:CustomerSCD2Main {customer_id: $customer_id})
                ON CREATE SET m.created_at = datetime()
                
                CREATE (v:CustomerSCD2Version {
                    full_name: $full_name,
                    email: $email,
                    phone: $phone,
                    city: $city,
                    address: $address,
                    status: $status,
                    sign_up_date: date($sign_up_date),
                    age: $age,
                    subscription_type: $subscription_type,
                    valid_from: date($sign_up_date),
                    valid_to: NULL,
                    is_current: true
                })
                MERGE (m)-[:HAS_VERSION]->(v)
                """,
                customer_id=customer_id, full_name=full_name_new, email=email_new,
                phone=phone_new, city=city_new, address=address_new,
                status=status_new, sign_up_date=row['sign_up_date'], age=age_new,
                subscription_type=subscription_type_new)
                print(f"Inserted new customer_id={customer_id} as new SCD2 version.")
            else:
                # Detect changes
                changes = (
                    email_old != email_new or
                    phone_old != phone_new or
                    city_old != city_new or
                    address_old != address_new or
                    status_old != status_new or
                    subscription_type_old != subscription_type_new
                )

                if changes:
                    session.run("""
                    MATCH (m:CustomerSCD2Main {customer_id: $customer_id})-[:HAS_VERSION]->(v:CustomerSCD2Version {is_current:true})
                    SET v.is_current = false,
                        v.valid_to = date($today)
                    
                    CREATE (v2:CustomerSCD2Version {
                        full_name: $full_name,
                        email: $email,
                        phone: $phone,
                        city: $city,
                        address: $address,
                        status: $status,
                        sign_up_date: date($sign_up_date),
                        age: $age,
                        subscription_type: $subscription_type,
                        valid_from: date($today),
                        valid_to: NULL,
                        is_current: true
                    })
                    MERGE (m)-[:HAS_VERSION]->(v2)
                    """, customer_id=customer_id, today=TODAY, 
                    full_name=full_name_new, email=email_new, phone=phone_new,
                    city=city_new, address=address_new, status=status_new,
                    sign_up_date=row['sign_up_date'], age=age_new,
                    subscription_type=subscription_type_new)

                    print(f"SCD Type 2 Update for customer_id={customer_id}")
                else:
                    print(f"No changes for customer_id={customer_id}, no SCD2 update needed.")


if __name__ == "__main__":
    apply_scd_type2()
    print("Day 2 changes applied using SCD Type 2 for Neo4j.")
