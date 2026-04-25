## uruchom przez spark-submit streamrate.py

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("StreamingDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
)


query = (df.writeStream 
    .format("console") 
    .outputMode("append") 
    .option("truncate", False) 
    .start()
) 

query.awaitTermination()
