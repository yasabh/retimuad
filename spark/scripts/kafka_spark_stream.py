from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .getOrCreate()

# Read stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "images,logs,audio") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka key-value to string and display the output
stream_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Print the stream to console
query = stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Await termination
query.awaitTermination()
