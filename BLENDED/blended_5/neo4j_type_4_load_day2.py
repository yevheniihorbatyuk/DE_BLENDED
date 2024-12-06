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

def apply_scd_type4():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    with driver.session() as session:
        for row in day2_rows:
            customer_id = int(row['customer_id'])
            full_name_new = row['full_name']
            email_new = row['email']
            city_new = row['city']
            sign_up_date_new = row['sign_up_date']  # sign_up_date не змінюється зазвичай, але зберігаємо

            # Отримуємо поточний вузол
            # current_version: (m)-[:HAS_CURRENT]->(c:CustomerSCD4Current)
            result = session.run("""
            MATCH (m:CustomerSCD4Main {customer_id: $customer_id})-[:HAS_CURRENT]->(c:CustomerSCD4Current)
            RETURN c.email AS email_old, c.city AS city_old, c.full_name AS full_name_old, c.sign_up_date AS sign_up_date_old, c
            """, customer_id=customer_id)

            record = result.single()

            if record is None:
                # Новий клієнт - створимо main і current (без історії)
                session.run("""
                MERGE (m:CustomerSCD4Main {customer_id: $customer_id})
                ON CREATE SET m.created_at = datetime()

                CREATE (c:CustomerSCD4Current {
                    full_name: $full_name_new,
                    email: $email_new,
                    city: $city_new,
                    sign_up_date: date($sign_up_date_new)
                })

                MERGE (m)-[:HAS_CURRENT]->(c)
                """,
                customer_id=customer_id, full_name_new=full_name_new, email_new=email_new, city_new=city_new, sign_up_date_new=sign_up_date_new)
                print(f"Inserted new customer_id={customer_id} as new current (SCD4).")
            else:
                email_old = record["email_old"]
                city_old = record["city_old"]
                full_name_old = record["full_name_old"]
                sign_up_date_old = record["sign_up_date_old"]
                current_node = record["c"]  # цим можна ігнорувати, оскільки нам не треба коду з node

                changes = (email_old != email_new) or (city_old != city_new)

                if changes:
                    # Переносимо старі дані в історію
                    # valid_from = sign_up_date_old, valid_to = TODAY
                    # Створюємо історичний вузол
                    session.run("""
                    MATCH (m:CustomerSCD4Main {customer_id: $customer_id})-[:HAS_CURRENT]->(c:CustomerSCD4Current)
                    WITH m, c
                    CREATE (h:CustomerSCD4History {
                        full_name: c.full_name,
                        email: c.email,
                        city: c.city,
                        sign_up_date: c.sign_up_date,
                        valid_from: c.sign_up_date,
                        valid_to: date($today)
                    })
                    MERGE (m)-[:HAS_HISTORY]->(h)

                    // Оновлюємо current новими даними
                    SET c.full_name = $full_name_new,
                        c.email = $email_new,
                        c.city = $city_new
                    """,
                    customer_id=customer_id, today=TODAY, full_name_new=full_name_new, email_new=email_new, city_new=city_new)
                    print(f"SCD Type 4 Update for customer_id={customer_id}: old current moved to history, current updated.")
                else:
                    print(f"No changes for customer_id={customer_id}, no SCD4 update needed.")

if __name__ == "__main__":
    apply_scd_type4()
    print("Day 2 changes applied using SCD Type 4 for Neo4j.")
