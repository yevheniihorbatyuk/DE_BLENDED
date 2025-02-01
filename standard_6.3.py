import datetime
import uuid
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# 🔧 Налаштування для роботи з Kafka через PySpark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# 🔑 Конфігурація Kafka
KAFKA_BOOTSTRAP = "77.81.230.104:9092"
KAFKA_TOPIC_INPUT = "building_sensors_greenmoon"
KAFKA_TOPIC_OUTPUT = "alerts_stream"
KAFKA_USERNAME = "admin"
KAFKA_PASSWORD = "VawEzo1ikLtrA8Ug8THa"

# 🚀 Ініціалізація Spark Session
spark = (
    SparkSession.builder.appName("KafkaStreaming")
    .master("local[*]")  # Використання всіх ядер локального комп'ютера
    .config("spark.sql.debug.maxToStringFields", "200")
    .config("spark.sql.shuffle.partitions", "4")  # Оптимізація партицій
    .config("spark.streaming.backpressure.enabled", "true")  # Авто-регулювання навантаження
    .config("spark.sql.session.timeZone", "UTC")  # Корекція часових зон
    .getOrCreate()
)

# 📝 Завантаження CSV-файлу з умовами алертів
alerts_df = spark.read.option("header", True).csv("./data/alerts_conditions.csv")

# 🏗️ Схема JSON-повідомлень Kafka
json_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
])

# 🔌 Читання потоку з Kafka
raw_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";',
    )
    .option("subscribe", KAFKA_TOPIC_INPUT)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "500")  # Баланс між продуктивністю і затримкою
    .load()
)

# 📡 Декодування JSON із Kafka
sensor_data_df = (
    raw_stream_df.selectExpr("CAST(value AS STRING) AS json_string")
    .withColumn("data", from_json(col("json_string"), json_schema))
    .select("data.*")
    .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))  # Час у коректному форматі
)

# 🔍 Визначення часових вікон
windowed_df = (
    sensor_data_df.withWatermark("timestamp", "10 seconds")  # Врахування запізнілих даних
    .groupBy(window("timestamp", "1 minute", "30 seconds"))  # Віконна агрегація
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
    )
)

# 🛑 Фільтрація алертів (без `crossJoin`)
alerts_filtered = (
    windowed_df.join(
        alerts_df,
        (col("avg_temp") > col("temperature_min"))
        & (col("avg_temp") < col("temperature_max"))
        | (col("avg_humidity") > col("humidity_min"))
        & (col("avg_humidity") < col("humidity_max")),
        "inner",
    )
    .withColumn("alert_id", expr("uuid()"))
    .withColumn("alert_time", current_timestamp())
    .select(
        col("alert_id").alias("key"),
        to_json(
            struct(
                col("window"),
                col("avg_temp"),
                col("avg_humidity"),
                col("code"),
                col("message"),
                col("alert_time"),
            )
        ).alias("value"),
    )
)

# 🖥️ Виведення алертів у консоль (тільки для дебагу)
console_output = (
    alerts_filtered.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 10)
    .trigger(processingTime="10 seconds")
    .start()
)

# 📡 Запис алертів назад у Kafka
kafka_output = (
    alerts_filtered.writeStream.outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";',
    )
    .option("topic", KAFKA_TOPIC_OUTPUT)
    .trigger(processingTime="10 seconds")
    .start()
)

# 🏁 Очікування завершення стріму
console_output.awaitTermination()
kafka_output.awaitTermination()
