from prefect import flow, task
from pyspark.sql import SparkSession
import os
import requests

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
def process_table(table):
    """Process a single table: read CSV, convert to Parquet, and save."""
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

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

@flow
def landing_to_bronze():
    """Main flow for processing tables from landing to bronze."""
    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        download_data(table)
        process_table(table)

if __name__ == "__main__":
    landing_to_bronze()
