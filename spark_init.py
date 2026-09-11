from pyspark.sql import SparkSession
import os

try:
    # 1. Initialize your SparkSession EXACTLY ONCE with the known version parameters
    from pyspark.sql import SparkSession

    def get_spark_session():
        return SparkSession.builder \
            .master("local[*]") \
            .appName("ClickstreamProcessing") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
            .getOrCreate()


    print("Successfully initialized Spark 4.2.0 with the Kafka package!")

except Exception as e:
    print(f"exception details:{e}")