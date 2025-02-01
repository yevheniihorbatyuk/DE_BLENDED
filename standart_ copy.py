import os
import requests
from pathlib import Path
from prefect import task, flow
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from config import kafka_config

# Константи
MYSQL_URL = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
MYSQL_PROPERTIES = {
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver"
}
KAFKA_BOOTSTRAP_SERVERS = kafka_config["bootstrap_servers"]
KAFKA_TOPIC_INPUT = "Ilya_athlete_topic_input"
KAFKA_TOPIC_OUTPUT = "Ilya_athlete_topic_output"

# 📥 Автоматичне завантаження mysql-connector.jar
MYSQL_CONNECTOR_PATH = "mysql-connector-java-8.0.33.jar"
MYSQL_CONNECTOR_URL = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar"

if not Path(MYSQL_CONNECTOR_PATH).exists():
    print(f"🔍 Завантаження {MYSQL_CONNECTOR_PATH} ...")
    response = requests.get(MYSQL_CONNECTOR_URL)
    with open(MYSQL_CONNECTOR_PATH, "wb") as f:
        f.write(response.content)
    print("✅ Завантаження завершено.")

@task
def initialize_spark():
    spark = SparkSession.builder \
        .appName("Final_project") \
        .config("spark.jars", MYSQL_CONNECTOR_PATH) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    return spark

@task
def read_athlete_bio(spark):
    return spark.read.jdbc(url=MYSQL_URL, table="olympic_dataset.athlete_bio", properties=MYSQL_PROPERTIES)

@task
def filter_athlete_bio(athlete_bio_df):
    return athlete_bio_df.filter(
        (col("height").isNotNull()) & 
        (col("weight").isNotNull()) & 
        (col("height").cast("float").isNotNull()) & 
        (col("weight").cast("float").isNotNull())
    )

@task
def read_event_result(spark):
    return spark.read.jdbc(url=MYSQL_URL, table="olympic_dataset.athlete_event_results", properties=MYSQL_PROPERTIES)

@task
def write_to_kafka(df):
    df.selectExpr("CAST(athlete_id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC_INPUT) \
        .save()

@task
def read_from_kafka(spark):
    schema = StructType([
        StructField("edition", StringType(), True),
        StructField("edition_id", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", StringType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", StringType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True)
    ])
    
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_INPUT) \
        .load()

    return kafka_stream_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

@task
def join_data(filtered_athlete_bio_df, parsed_kafka_df):
    return parsed_kafka_df.join(filtered_athlete_bio_df, "athlete_id", "inner")

@task
def calculate_aggregates(joined_df):
    return joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight")
        ) \
        .withColumn("timestamp", current_timestamp())

@task
def write_aggregated_to_kafka(aggregated_df):
    aggregated_df.selectExpr("CAST(sport AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC_OUTPUT) \
        .outputMode("append") \
        .start() \
        .awaitTermination()

@task
def write_aggregated_to_mysql(aggregated_df):
    def write_to_mysql(batch_df, batch_id):
        batch_df.write \
            .jdbc(url=MYSQL_URL, table="olympic_dataset.aggregated_results", mode="append", properties=MYSQL_PROPERTIES)
    
    aggregated_df.writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("append") \
        .start() \
        .awaitTermination()

@flow(name="Final_project_pipeline")
def main_flow():
    spark = initialize_spark()
    athlete_bio_df = read_athlete_bio(spark)
    event_results_df = read_event_result(spark)
    filtered_athlete_bio_df = filter_athlete_bio(athlete_bio_df)
    write_to_kafka(event_results_df)
    parsed_kafka_df = read_from_kafka(spark)
    joined_df = join_data(filtered_athlete_bio_df, parsed_kafka_df)
    aggregated_df = calculate_aggregates(joined_df)
    write_aggregated_to_kafka(aggregated_df)
    write_aggregated_to_mysql(aggregated_df)

if __name__ == "__main__":
    main_flow()
