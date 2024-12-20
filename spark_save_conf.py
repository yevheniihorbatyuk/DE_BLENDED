from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SaveConfig").getOrCreate()

with open("spark_configurations.txt", "w") as file:
    for key, value in spark.sparkContext.getConf().getAll():
        file.write(f"{key} = {value}\n")

spark.stop()