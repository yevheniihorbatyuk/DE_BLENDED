from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType
import os

@task
def clean_text(df):
    """Clean text columns by trimming whitespace and converting to lowercase."""
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, trim(lower(col(column))))
    return df

@task
def process_table(table):
    """Process a single table: clean text, drop duplicates, and save to silver layer."""
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    # Read the table from the bronze layer
    input_path = f"/tmp/bronze/{table}"
    df = spark.read.parquet(input_path)

    # Clean text and drop duplicates
    df = clean_text(df)
    df = df.dropDuplicates()

    # Save the cleaned data to the silver layer
    output_path = f"/tmp/silver/{table}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to {output_path}")

    # Optional: Re-read and verify
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

@flow
def bronze_to_silver():
    """Main flow to process tables from bronze to silver."""
    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        process_table(table)

if __name__ == "__main__":
    bronze_to_silver()
