from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType
import os

def create_spark_session():
    """Створює та повертає SparkSession."""
    return SparkSession.builder \
        .appName("BronzeToSilver") \
        .getOrCreate()

@task(cache_policy="NO_CACHE")
def clean_text(table: str):
    """Очищає текстові колонки у таблиці."""
    spark = create_spark_session()
    input_path = f"/tmp/bronze/{table}"
    
    # Читаємо таблицю
    df = spark.read.parquet(input_path)

    # Очистка текстових колонок
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, trim(lower(col(column))))

    return df

@task(cache_policy="NO_CACHE")
def process_table(table: str):
    """Обробляє таблицю: очищає текст, видаляє дублі та зберігає у silver."""
    spark = create_spark_session()

    # Читаємо таблицю
    input_path = f"/tmp/bronze/{table}"
    df = spark.read.parquet(input_path)

    # Викликаємо очищення
    df = clean_text(table)

    # Видаляємо дублі
    df = df.dropDuplicates()

    # Зберігаємо у Silver
    output_path = f"/tmp/silver/{table}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    print(f"✅ Дані збережено у {output_path}")

    # Перевіряємо результат
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

@flow
def bronze_to_silver():
    """Обробляє всі таблиці з bronze до silver."""
    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        process_table(table)

if __name__ == "__main__":
    bronze_to_silver()
