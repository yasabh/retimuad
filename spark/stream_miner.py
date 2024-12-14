import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Kafka and InfluxDB configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")

# Load Pre-trained Model
MODEL_PATH = "iomt"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IoMT_RealTime_Prediction") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Update with the path to your trained model
model = LinearRegressionModel.load(MODEL_PATH)

# Define Kafka Stream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Define Schema for Incoming Data
schema = StructType([
    StructField("SrcBytes", IntegerType(), True),
    StructField("DstBytes", IntegerType(), True),
    StructField("SrcLoad", FloatType(), True),
    StructField("DstLoad", FloatType(), True),
    StructField("Temp", FloatType(), True),
    StructField("SpO2", IntegerType(), True),
    StructField("Pulse_Rate", IntegerType(), True),
    StructField("SYS", IntegerType(), True),
    StructField("DIA", IntegerType(), True),
    StructField("Heart_rate", IntegerType(), True)
])

# Parse JSON from Kafka
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Assemble Features
assembler = VectorAssembler(
    inputCols=["SrcBytes", "DstBytes", "SrcLoad", "DstLoad", "Temp", "SpO2", "Pulse_Rate", "SYS", "DIA", "Heart_rate"],
    outputCol="features"
)
feature_df = assembler.transform(parsed_df)

# Predict Using Pre-trained Model
predictions = model.transform(feature_df)

# Write Predictions to InfluxDB
def write_to_influxdb(row):
    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org="org") as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        point = Point("IoMT") \
            .field("prediction", float(row["prediction"])) \
            .field("SrcBytes", row["SrcBytes"]) \
            .field("DstBytes", row["DstBytes"]) \
            .field("SrcLoad", row["SrcLoad"]) \
            .field("DstLoad", row["DstLoad"]) \
            .field("Temp", row["Temp"]) \
            .field("SpO2", row["SpO2"]) \
            .field("Pulse_Rate", row["Pulse_Rate"]) \
            .field("SYS", row["SYS"]) \
            .field("DIA", row["DIA"]) \
            .field("Heart_rate", row["Heart_rate"])
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)

# Stream Processing and Writing to InfluxDB
query = predictions.select("SrcBytes", "DstBytes", "SrcLoad", "DstLoad", "Temp", "SpO2", "Pulse_Rate", "SYS", "DIA", "Heart_rate", "prediction") \
    .writeStream \
    .foreach(write_to_influxdb) \
    .start()

query.awaitTermination()
