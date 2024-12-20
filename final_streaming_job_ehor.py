import os
import logging
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    avg, col, current_timestamp, from_json, to_json, struct, regexp_replace,
    coalesce, lit, isnan
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class KafkaConfig:
    bootstrap_servers: list[str]  # Changed to list for consistency
    username: str
    password: str
    security_protocol: str
    sasl_mechanism: str
    topic_prefix: str

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
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            topic_prefix=os.getenv("KAFKA_TOPIC_PREFIX", "greenmoon_end")
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

class SparkStreamProcessor:
    def __init__(self, checkpoint_dir: str = "checkpoint"):
        self.kafka_config = KafkaConfig.from_env()
        self.mysql_config = MySQLConfig.from_env()
        self.checkpoint_dir = checkpoint_dir
        self.spark = self._create_spark_session()
        self.schema = self._create_schema()

    def _create_spark_session(self) -> SparkSession:
        mysql_jar_path = os.path.abspath("mysql-connector-j-8.3.0.jar")
        
        if not os.path.exists(mysql_jar_path):
            raise FileNotFoundError(
                f"MySQL connector JAR not found at {mysql_jar_path}. "
                "Please download it using:\n"
                "wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar"
            )

        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
            "pyspark-shell"
        )

        return (
            SparkSession.builder
            .config("spark.jars", mysql_jar_path)
            .config("spark.driver.extraClassPath", mysql_jar_path)
            .config("spark.executor.extraClassPath", mysql_jar_path)
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_dir)
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .appName("EnhancedStreamingJob")
            .master("local[*]")
            .getOrCreate()
        )

    def _create_schema(self) -> StructType:
        return StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

    def read_from_mysql(self, table: str, partition_column: str) -> DataFrame:
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

    def process_stream(self) -> None:
        try:
            # Read athlete event results from MySQL
            jdbc_df = self.read_from_mysql("athlete_event_results", "result_id")
            
            # Write initial data to Kafka
            self.write_to_kafka(jdbc_df, f"{self.kafka_config.topic_prefix}_athlete_event_results")
            
            # Read from Kafka stream
            kafka_df = (
                self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", ",".join(self.kafka_config.bootstrap_servers))
                .option("kafka.security.protocol", self.kafka_config.security_protocol)
                .option("kafka.sasl.mechanism", self.kafka_config.sasl_mechanism)
                .option("kafka.sasl.jaas.config", self.kafka_config.sasl_jaas_config)
                .option("subscribe", f"{self.kafka_config.topic_prefix}_athlete_event_results")
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", 5)
                .load()
                .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
                .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), self.schema).alias("data"))
                .select("data.*")
            )
            
            # Read and process athlete biography data
            athlete_bio_df = self.read_from_mysql("athlete_bio", "athlete_id")
            
            # Process and aggregate data
            aggregated_df = (
                kafka_df.join(athlete_bio_df, "athlete_id")
                .groupBy("sport", "medal")
                .agg(
                    coalesce(avg("height"), lit(0.0)).alias("avg_height"),
                    coalesce(avg("weight"), lit(0.0)).alias("avg_weight"),
                    current_timestamp().alias("timestamp")
                )
                .filter(
                    ~isnan(col("avg_height")) 
                    & ~isnan(col("avg_weight"))
                    & col("avg_height").isNotNull()
                    & col("avg_weight").isNotNull()
                )
            )

            def foreach_batch_function(batch_df: DataFrame, epoch_id: int) -> None:
                try:
                    # Print the batch to console
                    logger.info(f"\nProcessing batch {epoch_id}:")
                    logger.info("Batch Schema:")
                    batch_df.printSchema()
                    logger.info("\nBatch Data:")
                    batch_df.show(truncate=False)
                    
                    # Write to Kafka
                    self.write_to_kafka(
                        batch_df, 
                        f"{self.kafka_config.topic_prefix}_enriched_athlete_avg"
                    )
                    
                    # Write to MySQL
                    (batch_df.write
                    .format("jdbc")
                    .option("url", self.mysql_config.jdbc_url)
                    .option("driver", self.mysql_config.driver)
                    .option("dbtable", f"{self.kafka_config.topic_prefix}_enriched_athlete_avg")
                    .option("user", self.mysql_config.user)
                    .option("password", self.mysql_config.password)
                    .mode("append")
                    .save())
                    
                    logger.info(f"Batch {epoch_id} processed successfully")
                except Exception as e:
                    logger.error(f"Error in batch {epoch_id}: {str(e)}")
                    raise

            # Console output stream
            query1 = (aggregated_df.writeStream
                    .outputMode("complete")
                    .format("console")
                    .option("truncate", False)
                    .option("numRows", 20)
                    .start())

            # Main processing stream
            query2 = (aggregated_df.writeStream
                    .outputMode("complete")
                    .foreachBatch(foreach_batch_function)
                    .option("checkpointLocation", os.path.join(self.checkpoint_dir, "streaming"))
                    .start())

            # Wait for both streams to terminate
            query1.awaitTermination()
            query2.awaitTermination()

        except Exception as e:
            logger.error(f"Error in stream processing: {str(e)}")
            raise

def main():
    try:
        processor = SparkStreamProcessor()
        processor.process_stream()
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()