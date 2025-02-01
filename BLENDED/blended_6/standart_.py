import os
import requests
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from configs import kafka_config

# 📌 Налаштування MySQL
MYSQL_URL = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
MYSQL_PROPERTIES = {
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# 📌 Налаштування Kafka
KAFKA_BOOTSTRAP_SERVERS = kafka_config["bootstrap_servers"][0]
KAFKA_TOPIC_INPUT = "Ilya_athlete_topic_input"
KAFKA_TOPIC_OUTPUT = "Ilya_athlete_topic_output"

# 📥 Завантаження MySQL Connector JAR (якщо відсутній)
MYSQL_CONNECTOR_PATH = "mysql-connector-java-8.0.33.jar"
MYSQL_CONNECTOR_URL = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar"

if not Path(MYSQL_CONNECTOR_PATH).exists():
    print(f"🔍 Завантаження {MYSQL_CONNECTOR_PATH} ...")
    response = requests.get(MYSQL_CONNECTOR_URL)
    with open(MYSQL_CONNECTOR_PATH, "wb") as f:
        f.write(response.content)
    print("✅ Завантаження завершено.")

# 📌 Ініціалізація Spark
spark = SparkSession.builder \
    .appName("Final_project") \
    .config("spark.jars", MYSQL_CONNECTOR_PATH) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# 1️⃣ Читання біографічних даних атлетів
athlete_bio_df = spark.read.format("jdbc").options(
    url=MYSQL_URL,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="olympic_dataset.athlete_bio",
    user=MYSQL_PROPERTIES["user"],
    password=MYSQL_PROPERTIES["password"]
).load()

# 2️⃣ Фільтрація некоректних значень
athlete_bio_cleaned_df = athlete_bio_df.filter(
    (col("height").isNotNull()) & (col("weight").isNotNull()) &
    (col("height").cast("double").isNotNull()) & (col("weight").cast("double").isNotNull())
)

# 3️⃣ Читання та запис результатів змагань у Kafka
event_results_df = spark.read.format("jdbc").options(
    url=MYSQL_URL,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="olympic_dataset.athlete_event_results",
    user=MYSQL_PROPERTIES["user"],
    password=MYSQL_PROPERTIES["password"]
).load()

event_results_df.selectExpr("CAST(athlete_id AS STRING) as key", "to_json(struct(*)) AS value") \
    .write.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", KAFKA_TOPIC_INPUT) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";') \
    .save()

# 📌 Читання з Kafka (потоково)
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

kafka_stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC_INPUT) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";') \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "500") \
    .load()

kafka_json_df = kafka_stream_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# 4️⃣ Об’єднання з біографічними даними
joined_stream_df = kafka_json_df.join(athlete_bio_cleaned_df, on="athlete_id", how="inner")

# 5️⃣ Агрегація за видом спорту, медаллю, статтю, країною
aggregated_stream_df = joined_stream_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp")
)

# ✅ Функція запису у Kafka та MySQL
def foreach_batch_function(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.selectExpr("to_json(struct(*)) AS value") \
            .write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", KAFKA_TOPIC_OUTPUT) \
            .option("kafka.security.protocol", kafka_config["security_protocol"]) \
            .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" '
                    f'password="{kafka_config["password"]}";') \
            .save()

        batch_df.write.format("jdbc").options(
            url=MYSQL_URL,
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="olympic_dataset.aggregated_results",
            user=MYSQL_PROPERTIES["user"],
            password=MYSQL_PROPERTIES["password"]
        ).mode("append").save()

# 6️⃣ Старт стріму
aggregated_stream_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "checkpoint/athlete_pipeline") \
    .start() \
    .awaitTermination()
