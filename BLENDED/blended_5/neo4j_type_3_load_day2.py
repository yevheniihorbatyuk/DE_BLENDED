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

def apply_scd_type3():
    with open(DAY2_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        day2_rows = list(reader)

    with driver.session() as session:
        for row in day2_rows:
            customer_id = int(row['customer_id'])
            full_name_new = row['full_name']
            email_new = row['email']
            city_new = row['city']
            sign_up_date_new = row['sign_up_date']  # sign_up_date зазвичай не змінюємо

            # Отримуємо поточний запис
            result = session.run("""
            MATCH (c:CustomerSCD3 {customer_id: $customer_id})
            RETURN c.email AS email_old, c.previous_email AS prev_email_old,
                   c.city AS city_old, c.previous_city AS prev_city_old,
                   c.sign_up_date AS sign_up_date_old, c.full_name AS full_name_old
            """, customer_id=customer_id)

            record = result.single()

            if record is None:
                # Новий клієнт - створюємо новий вузол з no previous history
                session.run("""
                MERGE (c:CustomerSCD3 {customer_id: $customer_id})
                ON CREATE SET c.full_name = $full_name,
                              c.email = $email,
                              c.previous_email = NULL,
                              c.city = $city,
                              c.previous_city = NULL,
                              c.sign_up_date = date($sign_up_date),
                              c.last_update_date = date($today)
                """, customer_id=customer_id, full_name=full_name_new, email=email_new, city=city_new, sign_up_date=sign_up_date_new, today=TODAY)
                print(f"Inserted new customer_id={customer_id} (SCD3).")
            else:
                email_old = record["email_old"]
                city_old = record["city_old"]
                # prev_email_old = record["prev_email_old"]  # не обов'язково зараз
                # prev_city_old = record["prev_city_old"]
                # full_name_old = record["full_name_old"]
                sign_up_date_old = record["sign_up_date_old"]  # зберігаємо оригінальну дату реєстрації

                email_changed = (email_old != email_new)
                city_changed = (city_old != city_new)

                if email_changed or city_changed:
                    # Якщо email змінився:
                    # previous_email = email_old, email = email_new
                    # Якщо city змінився:
                    # previous_city = city_old, city = city_new

                    # Якщо атрибут не змінився, залишаємо його як було
                    session.run("""
                    MATCH (c:CustomerSCD3 {customer_id: $customer_id})
                    SET c.full_name = $full_name_new,
                        c.previous_email = CASE WHEN $email_changed THEN c.email ELSE c.previous_email END,
                        c.email = CASE WHEN $email_changed THEN $email_new ELSE c.email END,
                        c.previous_city = CASE WHEN $city_changed THEN c.city ELSE c.previous_city END,
                        c.city = CASE WHEN $city_changed THEN $city_new ELSE c.city END,
                        c.last_update_date = date($today)
                    """, customer_id=customer_id, full_name_new=full_name_new, email_changed=email_changed, email_new=email_new, city_changed=city_changed, city_new=city_new, today=TODAY)
                    
                    print(f"SCD Type 3 Update for customer_id={customer_id}")
                else:
                    print(f"No changes for customer_id={customer_id}, no SCD3 update needed.")

if __name__ == "__main__":
    apply_scd_type3()
    print("Day 2 changes applied using SCD Type 3 for Neo4j.")
