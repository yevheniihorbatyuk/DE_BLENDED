from pyspark.sql import SparkSession

# Ініціалізація SparkSession
spark = SparkSession.builder \
    .appName("ShowSparkConfig") \
    .getOrCreate()

# Показати всі конфігурації Spark
configurations = spark.sparkContext.getConf().getAll()

# Вивести конфігурації
for key, value in configurations:
    print(f"{key} = {value}")

# Зупинити сесію
spark.stop()
