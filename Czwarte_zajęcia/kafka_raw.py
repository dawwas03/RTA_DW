from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2

spark = (
    SparkSession.builder
    .appName("Lab4-Kafka")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "transactions")
    .load()
)

q = (
    kafka_raw.writeStream
    .format("console") 
    .outputMode("append") 
    .option("truncate", False)
    .start()
)

q.awaitTermination()
