from prefect import flow, task
# from prefect.tasks.shell import ShellOperation
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, avg, current_timestamp
from pyspark.sql.types import StringType
import requests

def get_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

@task
def download_data(file):
    """Download data from the server and save it locally as a CSV file."""
    url = f"https://ftp.goit.study/neoversity/{file}.csv"
    print(f"Downloading from {url}")
    response = requests.get(url, timeout=10)

    if response.status_code == 200:
        with open(f"{file}.csv", 'wb') as f:
            f.write(response.content)
        print(f"File {file}.csv downloaded successfully.")
    else:
        raise Exception(f"Failed to download {file}. Status code: {response.status_code}")

@task
def process_landing_to_bronze(table):
    """Process a single table: read CSV, convert to Parquet, and save."""
    spark = get_spark_session("LandingToBronze")

    local_path = f"{table}.csv"
    output_path = f"/tmp/bronze/{table}"

    # Read the CSV into a Spark DataFrame
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)

    # Write the DataFrame to Parquet
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")

    # Optional: Re-read and verify
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

@task
def clean_text(df):
    """Clean text columns by trimming whitespace and converting to lowercase."""
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, trim(lower(col(column))))
    return df

@task
def process_bronze_to_silver(table):
    """Process a single table: clean text, drop duplicates, and save to silver layer."""
    spark = get_spark_session("BronzeToSilver")

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

@task
def process_silver_to_gold():
    """Process data from silver to gold layer."""
    spark = get_spark_session("SilverToGold")

    # Load tables from the silver layer
    athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
    athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

    # Rename columns to avoid ambiguity when joining
    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

    # Join the tables on the "athlete_id" column
    joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

    # Calculate average values for each group
    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
            current_timestamp().alias("timestamp")
        )

    # Create a directory to save the results in the gold layer
    output_path = "/tmp/gold/avg_stats"
    os.makedirs(output_path, exist_ok=True)

    # Save the processed data in parquet format
    aggregated_df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to {output_path}")

    # Optional: Re-read and verify
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

@flow
def etl_pipeline():
    """End-to-end ETL pipeline from landing to gold."""
    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        download_data(table)
        process_landing_to_bronze(table)
        process_bronze_to_silver(table)

    process_silver_to_gold()

if __name__ == "__main__":
    # etl_pipeline.visualize()
    etl_pipeline.serve(name="etl_pipeline", cron="*/10 * * * *")
