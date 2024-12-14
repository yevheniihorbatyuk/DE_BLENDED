from dataclasses import dataclass
from typing import List, Optional
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.dataframe import DataFrame
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class KafkaConfig:
    bootstrap_servers: List[str]
    username: str
    password: str
    security_protocol: str
    sasl_mechanism: str

    @property
    def sasl_jaas_config(self) -> str:
        return (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{self.username}" password="{self.password}";'
        )

    @classmethod
    def from_env(cls) -> 'KafkaConfig':
        return cls(
            bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "77.81.230.104:9092")],
            username=os.getenv("KAFKA_USERNAME", "admin"),
            password=os.getenv("KAFKA_PASSWORD", "VawEzo1ikLtrA8Ug8THa"),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
        )

@dataclass
class MySQLConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    driver: str = "com.mysql.cj.jdbc.Driver"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}"

    @classmethod
    def from_env(cls) -> 'MySQLConfig':
        return cls(
            host=os.getenv("MYSQL_HOST", "217.61.57.46"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", "olympic_dataset"),
            user=os.getenv("MYSQL_USER", "neo_data_admin"),
            password=os.getenv("MYSQL_PASSWORD", "Proyahaxuqithab9oplp")
        )

class SparkKafkaProcessor:
    def __init__(
        self,
        kafka_config: KafkaConfig,
        mysql_config: MySQLConfig,
        checkpoint_dir: str = "checkpoint"
    ):
        self.kafka_config = kafka_config
        self.mysql_config = mysql_config
        self.checkpoint_dir = checkpoint_dir
        self.spark = self._create_spark_session()
        self.schema = self._create_schema()
            
    def _create_spark_session(self) -> SparkSession:
        """Initialize Spark session with required configurations."""
        # Define MySQL connector path and verify its existence
        mysql_jar_path = os.path.abspath("mysql-connector-j-8.3.0.jar")
        
        if not os.path.exists(mysql_jar_path):
            raise FileNotFoundError(
                f"MySQL connector JAR not found at {mysql_jar_path}. "
                "Please download it using:\n"
                "wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar"
            )

        # Update MySQL driver class name for newer connector
        self.mysql_config.driver = "com.mysql.cj.jdbc.Driver"

        # Set up Spark packages
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
            "pyspark-shell"
        )

        # Create and configure Spark session
        spark = (
            SparkSession.builder
            .config("spark.jars", mysql_jar_path)
            .config("spark.driver.extraClassPath", mysql_jar_path)
            .config("spark.executor.extraClassPath", mysql_jar_path)
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_dir)
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.driver.memory", "2g")  # Increased memory for better performance
            .config("spark.executor.memory", "2g")
            .appName("EnhancedJDBCToKafka")
            .master("local[*]")
            .getOrCreate()
        )

        # Verify MySQL driver is loaded
        try:
            spark.sparkContext._jvm.Class.forName("com.mysql.cj.jdbc.Driver")
            logger.info("MySQL driver 8.3.0 successfully loaded")
        except Exception as e:
            logger.error(f"Failed to load MySQL driver: {e}")
            raise

        return spark

    def _create_schema(self) -> StructType:
        """Create schema for Kafka messages."""
        return StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("timestamp", StringType(), True),
        ])

    def read_from_mysql(self, table: str, partition_column: str) -> DataFrame:
        """Read data from MySQL with error handling."""
        try:
            return (
                self.spark.read.format("jdbc")
                .options(
                    url=self.mysql_config.jdbc_url,
                    driver=self.mysql_config.driver,
                    dbtable=table,
                    user=self.mysql_config.user,
                    password=self.mysql_config.password,
                    partitionColumn=partition_column,
                    lowerBound=1,
                    upperBound=1000000,
                    numPartitions=10
                )
                .load()
            )
        except Exception as e:
            logger.error(f"Error reading from MySQL table {table}: {str(e)}")
            raise

    def write_to_kafka(self, df: DataFrame, topic: str) -> None:
        """Write DataFrame to Kafka topic."""
        try:
            (df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
             .write
             .format("kafka")
             .option("kafka.bootstrap.servers", ",".join(self.kafka_config.bootstrap_servers))
             .option("kafka.security.protocol", self.kafka_config.security_protocol)
             .option("kafka.sasl.mechanism", self.kafka_config.sasl_mechanism)
             .option("kafka.sasl.jaas.config", self.kafka_config.sasl_jaas_config)
             .option("topic", topic)
             .save())
        except Exception as e:
            logger.error(f"Error writing to Kafka topic {topic}: {str(e)}")
            raise

    def process_stream(self, topic_prefix: str = "greenmoon_end") -> None:
        """Main processing logic."""
        try:
            # Read athlete event results from MySQL
            jdbc_df = self.read_from_mysql("athlete_event_results", "result_id")
            
            # Write to Kafka
            self.write_to_kafka(jdbc_df, f"{topic_prefix}_athlete_event_results")

            # Read from Kafka stream
            kafka_df = self._read_kafka_stream(f"{topic_prefix}_athlete_event_results")
            
            # Read and process athlete biography data
            athlete_bio_df = self._process_athlete_bio()
            
            # Process and aggregate data
            aggregated_df = self._aggregate_data(kafka_df, athlete_bio_df)
            
            # Start streaming with error handling
            self._start_streaming(aggregated_df, topic_prefix)

        except Exception as e:
            logger.error(f"Error in stream processing: {str(e)}")
            raise

    def _read_kafka_stream(self, topic: str) -> DataFrame:
        """Configure and read Kafka stream."""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(self.kafka_config.bootstrap_servers))
            .option("kafka.security.protocol", self.kafka_config.security_protocol)
            .option("kafka.sasl.mechanism", self.kafka_config.sasl_mechanism)
            .option("kafka.sasl.jaas.config", self.kafka_config.sasl_jaas_config)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "5")
            .option("failOnDataLoss", "false")
            .load()
            .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
            .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), self.schema).alias("data"))
            .select("data.athlete_id", "data.sport", "data.medal")
        )

    def _process_athlete_bio(self) -> DataFrame:
        """Process athlete biography data."""
        df = self.read_from_mysql("athlete_bio", "athlete_id")
        return df.filter(
            (col("height").isNotNull())
            & (col("weight").isNotNull())
            & (col("height").cast("double").isNotNull())
            & (col("weight").cast("double").isNotNull())
        )

    def _aggregate_data(self, stream_df: DataFrame, bio_df: DataFrame) -> DataFrame:
        """Aggregate streaming data with biography data."""
        return (
            stream_df.join(bio_df, "athlete_id")
            .groupBy("sport", "medal", "sex", "country_noc")
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
                current_timestamp().alias("timestamp")
            )
        )

    def _start_streaming(self, df: DataFrame, topic_prefix: str) -> None:
        """Start the streaming job with proper error handling and console output."""
        def foreach_batch_function(batch_df: DataFrame, epoch_id: int) -> None:
            try:
                # Write to Kafka
                self.write_to_kafka(batch_df, f"{topic_prefix}_enriched_athlete_avg")

                # Write to MySQL
                (batch_df.write
                .format("jdbc")
                .options(
                    url="jdbc:mysql://217.61.57.46:3306/neo_data",
                    driver=self.mysql_config.driver,
                    dbtable=f"{topic_prefix}_enriched_athlete_avg",
                    user=self.mysql_config.user,
                    password=self.mysql_config.password
                )
                .mode("append")
                .save())

                logger.info(f"Batch processed successfully for epoch {epoch_id}.")
            except Exception as e:
                logger.error(f"Error in batch processing (epoch {epoch_id}): {str(e)}")
                raise

        # Streaming to console output
        (df.writeStream
        .outputMode("complete")  # Use 'complete' or 'append' depending on requirements
        .format("console")
        .option("truncate", "false")  # Full output without truncation
        .option("numRows", 50)  # Number of rows to display
        .start())

        # Main streaming logic with foreachBatch
        (df.writeStream
        .outputMode("complete")
        .foreachBatch(foreach_batch_function)
        .option("checkpointLocation", os.path.join(self.checkpoint_dir, "streaming"))
        .start()
        .awaitTermination())

def main():
    """Main entry point."""
    try:
        # Initialize configurations
        kafka_config = KafkaConfig.from_env()
        mysql_config = MySQLConfig.from_env()
        
        # Create and run processor
        processor = SparkKafkaProcessor(kafka_config, mysql_config)
        processor.process_stream()
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()