import os
import csv
from dotenv import load_dotenv
from neo4j import GraphDatabase
from datetime import date

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4jratatui")

DAY2_FILE = "BLENDED/blended_5/data/day2_changes.csv"
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
            city_new = row['city']
            sign_up_date_new = row['sign_up_date']  # зазвичай це не змінюється, але зберігаємо як є

            # Отримуємо поточну версію
            # current_version: найдемо version з is_current=true для даного customer_id
            result = session.run("""
            MATCH (m:CustomerSCD2Main {customer_id: $customer_id})-[:HAS_VERSION]->(v:CustomerSCD2Version {is_current:true})
            RETURN v.email AS email_old, v.city AS city_old, v.sign_up_date AS sign_up_date_old, v.full_name AS full_name_old, v
            """, customer_id=customer_id)

            record = result.single()

            if record is None:
                # Нема поточної версії - новий клієнт?
                # Створимо main і version
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
                customer_id=customer_id, full_name=full_name_new, email=email_new, city=city_new, sign_up_date=sign_up_date_new)
                print(f"Inserted new customer_id={customer_id} as new SCD2 version.")
            else:
                email_old = record["email_old"]
                city_old = record["city_old"]
                full_name_old = record["full_name_old"]
                sign_up_date_old = record["sign_up_date_old"]
                current_version_node = record["v"]  # містить id для подальшого оновлення
                
                changes = (email_old != email_new) or (city_old != city_new)

                if changes:
                    # Оновлюємо стару версію: is_current=false, valid_to=TODAY
                    # Створюємо нову версію з актуальними даними
                    session.run("""
                    MATCH (m:CustomerSCD2Main {customer_id: $customer_id})-[:HAS_VERSION]->(v:CustomerSCD2Version {is_current:true})
                    SET v.is_current = false,
                        v.valid_to = date($today)
                    
                    CREATE (v2:CustomerSCD2Version {
                        full_name: $full_name_new,
                        email: $email_new,
                        city: $city_new,
                        sign_up_date: date($sign_up_date_old),
                        valid_from: date($today),
                        valid_to: NULL,
                        is_current: true
                    })
                    MERGE (m)-[:HAS_VERSION]->(v2)
                    """, customer_id=customer_id, today=TODAY, 
                    full_name_new=full_name_new, email_new=email_new, city_new=city_new,
                    sign_up_date_old=str(sign_up_date_old))  # sign_up_date_old приводимо до str

                    print(f"SCD Type 2 Update for customer_id={customer_id}")
                else:
                    print(f"No changes for customer_id={customer_id}, no SCD2 update needed.")

if __name__ == "__main__":
    apply_scd_type2()
    print("Day 2 changes applied using SCD Type 2 for Neo4j.")
