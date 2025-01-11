from dataclasses import dataclass
from typing import List, Optional
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, regexp_replace, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class KafkaConfig:
    """Kafka configuration settings."""
    bootstrap_servers: List[str]
    username: str
    password: str
    security_protocol: str
    sasl_mechanism: str
    topic: str
    max_offsets_per_trigger: int = 50

    @property
    def sasl_jaas_config(self) -> str:
        return (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{self.username}" password="{self.password}";'
        )

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

class KafkaStreamProcessor:
    """Process Kafka streams with Spark."""

    def __init__(self, kafka_config: KafkaConfig):
        """Initialize the processor with configurations."""
        self.kafka_config = kafka_config
        self.spark = self._create_spark_session()
        self.schema = self._create_schema()

    def _create_spark_session(self) -> SparkSession:
        """Initialize Spark session with required configurations."""
        # Set up Spark packages
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
            "pyspark-shell"
        )

        spark = (
            SparkSession.builder
            .appName("EnhancedKafkaStreaming")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .master("local[*]")
            .getOrCreate()
        )

        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _create_schema(self) -> StructType:
        """Create schema for Kafka messages."""
        return StructType([
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("sex", StringType(), True),
            StructField("noc_country", StringType(), True),
            StructField("avg_height", DoubleType(), True),
            StructField("avg_weight", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
        ])

    def _create_kafka_options(self) -> dict:
        """Create Kafka options for streaming."""
        return {
            "kafka.bootstrap.servers": ",".join(self.kafka_config.bootstrap_servers),
            "kafka.security.protocol": self.kafka_config.security_protocol,
            "kafka.sasl.mechanism": self.kafka_config.sasl_mechanism,
            "kafka.sasl.jaas.config": self.kafka_config.sasl_jaas_config,
            "subscribe": self.kafka_config.topic,
            "startingOffsets": "earliest",
            "maxOffsetsPerTrigger": str(self.kafka_config.max_offsets_per_trigger),
            "failOnDataLoss": "false"
        }

    def read_stream(self) -> DataFrame:
        """Read and process the Kafka stream."""
        try:
            return (
                self.spark.readStream
                .format("kafka")
                .options(**self._create_kafka_options())
                .load()
                .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
                .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), self.schema).alias("data"))
                .select("data.*")
            )
        except Exception as e:
            logger.error(f"Error reading Kafka stream: {e}")
            raise

    def process_stream(self, stream_df: DataFrame) -> None:
        """Process the streaming DataFrame."""
        try:
            query = (
                stream_df.writeStream
                .trigger(availableNow=True)
                .outputMode("append")
                .format("console")
                .option("truncate", "false")
                .start()
            )
            
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error processing stream: {e}")
            raise
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main execution function."""
    processor = None
    try:
        # Initialize configurations
        kafka_config = KafkaConfig.from_env()
        
        # Create processor
        processor = KafkaStreamProcessor(kafka_config)
        logger.info(f"Starting stream processing for topic: {kafka_config.topic}")
        
        # Process stream
        stream_df = processor.read_stream()
        processor.process_stream(stream_df)
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise
    finally:
        if processor:
            processor.stop()

if __name__ == "__main__":
    main()