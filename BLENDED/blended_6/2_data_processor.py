from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, current_timestamp, to_json, 
    struct, from_json, expr
)
from pyspark.sql.types import (
    StructType, StringType, IntegerType, 
    LongType, TimestampType
)
import os
from dataclasses import dataclass
from typing import List
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class KafkaConfig:
    """Kafka configuration class"""
    bootstrap_servers: List[str]
    username: str
    password: str
    security_protocol: str
    sasl_mechanism: str
    topic: str
    max_offsets_per_trigger: int

    @classmethod
    def from_env(cls) -> 'KafkaConfig':
        """Create configuration from environment variables."""
        return cls(
            bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "77.81.230.104:9092")],
            username=os.getenv("KAFKA_USERNAME", "admin"),
            password=os.getenv("KAFKA_PASSWORD", "VawEzo1ikLtrA8Ug8THa"),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            topic=os.getenv("KAFKA_TOPIC", "greenmoon_end_enriched_athlete_avg"),
            max_offsets_per_trigger=int(os.getenv("KAFKA_MAX_OFFSETS", "50"))
        )

@dataclass
class MySQLConfig:
    """MySQL configuration class"""
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> 'MySQLConfig':
        return cls(
            host=os.getenv("MYSQL_HOST", "217.61.57.46"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", "olympic_dataset"),
            user=os.getenv("MYSQL_USER", "neo_data_admin"),
            password=os.getenv("MYSQL_PASSWORD", "Proyahaxuqithab9oplp")
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}"

    @property
    def connection_properties(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

def get_table_names() -> dict:
    """Get table names configuration"""
    return {
        "athletes": "athletes_data",
        "results": "event_results",
        "aggregated": "ivan_y.aggregated_results"
    }

def create_spark_session(mysql_config: MySQLConfig):
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("ML Data Streaming Pipeline") \
        .config("spark.jars", "/path/to/mysql-connector-java.jar") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()

def get_kafka_options(kafka_config: KafkaConfig) -> dict:
    """Get Kafka options for Spark"""
    return {
        "kafka.bootstrap.servers": ",".join(kafka_config.bootstrap_servers),
        "kafka.sasl.mechanism": kafka_config.sasl_mechanism,
        "kafka.security.protocol": kafka_config.security_protocol,
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config.username}" password="{kafka_config.password}";',
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": str(kafka_config.max_offsets_per_trigger)
    }

def main():
    try:
        # Initialize configurations
        kafka_config = KafkaConfig.from_env()
        mysql_config = MySQLConfig.from_env()
        tables = get_table_names()
        
        spark = create_spark_session(mysql_config)
        logger.info("SparkSession successfully created.")

        # Read athlete data
        athlete_bio_df = spark.read \
            .jdbc(
                url=mysql_config.jdbc_url,
                table=tables["athletes"],
                properties=mysql_config.connection_properties
            )
        logger.info("Athlete bio data successfully loaded.")

        # Filter invalid values
        athlete_bio_filtered_df = athlete_bio_df.filter(
            col("height").cast("float").isNotNull() &
            col("weight").cast("float").isNotNull()
        )
        logger.info("Data successfully filtered.")

        # Read competition results
        athlete_event_results_df = spark.read \
            .jdbc(
                url=mysql_config.jdbc_url,
                table=tables["results"],
                properties=mysql_config.connection_properties
            )
        logger.info("Competition results successfully loaded.")

        # Prepare data for Kafka
        kafka_ready_df = athlete_event_results_df.select(
            col("result_id").cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        )
        logger.info("Data prepared for Kafka.")

        # Write to Kafka with new configuration
        kafka_options = get_kafka_options(kafka_config)
        kafka_ready_df.write \
            .format("kafka") \
            .options(**kafka_options) \
            .option("topic", kafka_config.topic) \
            .save()
        logger.info("Data successfully written to Kafka.")

        # Schema for JSON parsing
        schema = StructType([
            StructField("edition", StringType(), True),
            StructField("edition_id", IntegerType(), True),
            StructField("country_noc", StringType(), True),
            StructField("sport", StringType(), True),
            StructField("event", StringType(), True),
            StructField("result_id", LongType(), True),
            StructField("athlete", StringType(), True),
            StructField("athlete_id", IntegerType(), True),
            StructField("pos", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("isTeamSport", StringType(), True)
        ])

        # Read from Kafka
        kafka_stream_df = spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .option("subscribe", kafka_config.topic) \
            .load()
        logger.info("Kafka stream successfully initialized.")

        # Transform Kafka messages
        results_df = kafka_stream_df \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")
        logger.info("Kafka messages successfully transformed.")

        # Join data
        joined_df = results_df.join(
            athlete_bio_filtered_df,
            "athlete_id",
            "inner"
        ).select(
            results_df["sport"],
            results_df["medal"],
            athlete_bio_filtered_df["sex"],
            results_df["country_noc"],
            col("height").cast("float"),
            col("weight").cast("float")
        )
        logger.info("Data successfully joined.")

        # Calculate averages
        aggregated_df = joined_df \
            .groupBy("sport", "medal", "sex", "country_noc") \
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
                current_timestamp().alias("calculation_time")
            )
        logger.info("Averages successfully calculated.")

        # Save function
        def save_to_kafka_and_db(batch_df, batch_id):
            try:
                if batch_df.isEmpty():
                    logger.warning(f"Empty batch {batch_id}")
                    return

                # Write to Kafka
                batch_df.select(to_json(struct("*")).alias("value")) \
                    .write \
                    .format("kafka") \
                    .options(**kafka_options) \
                    .option("topic", kafka_config.topic) \
                    .save()
                logger.info(f"Batch {batch_id} written to Kafka")

                # Write to database
                batch_df.write \
                    .jdbc(
                        url=mysql_config.jdbc_url,
                        table=tables["aggregated"],
                        mode="append",
                        properties=mysql_config.connection_properties
                    )
                logger.info(f"Batch {batch_id} written to database")
            except Exception as e:
                logger.error(f"Error processing batch {batch_id}: {str(e)}")
                raise

        # Start streaming with checkpoint
        checkpoint_path = "/tmp/checkpoint/athlete_analytics"
        query = aggregated_df.writeStream \
            .outputMode("update") \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="10 seconds") \
            .foreachBatch(save_to_kafka_and_db) \
            .start()
        
        logger.info("Streaming query started.")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()