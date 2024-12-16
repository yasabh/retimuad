import os
import tensorflow as tf
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col, create_map, lit
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType
from pyspark.ml.feature import VectorAssembler
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from keras.layers import TFSMLayer

# Kafka and InfluxDB configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iomt-data")
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")

# Load Pre-trained Deep Learning Model (TensorFlow)
MODEL_PATH = os.getenv("MODEL_PATH")
model = TFSMLayer(MODEL_PATH, call_endpoint='serving_default')

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IoMT_RealTime_Prediction") \
    .master("local[*]") \
    .getOrCreate()

# Define Schema for Incoming Data
schema = StructType([
    StructField("SrcBytes", StringType(), True),
    StructField("DstBytes", StringType(), True),
    StructField("SrcLoad", StringType(), True),
    StructField("DstLoad", StringType(), True),
    StructField("Temp", StringType(), True),
    StructField("SpO2", StringType(), True),
    StructField("Pulse_Rate", StringType(), True),
    StructField("SYS", StringType(), True),
    StructField("DIA", StringType(), True),
    StructField("Heart_rate", StringType(), True),
    StructField("Dur", StringType(), True),
    StructField("TotBytes", StringType(), True),
    StructField("TotPkts", StringType(), True),
    StructField("Rate", StringType(), True),
    StructField("pLoss", StringType(), True),
    StructField("pSrcLoss", StringType(), True),
    StructField("pDstLoss", StringType(), True),
    StructField("SrcJitter", StringType(), True),
    StructField("DstJitter", StringType(), True),
    StructField("sMaxPktSz", StringType(), True),
    StructField("dMaxPktSz", StringType(), True),
    StructField("sMinPktSz", StringType(), True),
    StructField("dMinPktSz", StringType(), True),
    StructField("SrcGap", StringType(), True),
    StructField("DstGap", StringType(), True),
    StructField("Attack Category", StringType(), True)
])

# Parse JSON from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Map Attack Category to risk labels
category_mapping = {"normal": 0, "Spoofing": 1, "Data Alteration": 2}
mapping_expr = create_map([lit(x) for item in category_mapping.items() for x in item])
parsed_df = parsed_df.withColumn("Label", mapping_expr[col("Attack Category")])

# Cast string columns to appropriate data types
casted_df = parsed_df.select(
    *(col(c).cast("float").alias(c) if c not in ["Attack Category", "Label"] else col(c) for c in parsed_df.columns)
)

# Assemble Features
feature_cols = [col for col in casted_df.columns if col not in ["Attack Category", "Label"]]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
feature_df = assembler.transform(casted_df)

# Define prediction function
def predict_stream(batch_df, batch_id):
    if batch_df.count() > 0:
        # for row in batch_df.collect():
        #     print("batch_df", row)

        # Extract features and labels as NumPy arrays
        features = np.array(batch_df.select("features").rdd.map(lambda row: row[0]).collect())
        labels = np.array(batch_df.select("Label").rdd.flatMap(lambda x: x).collect())

        # Perform prediction
        predicted_classes = model(features)['output_0'].numpy().argmax(axis=1)  # For multi-class predictions
        # print("predicted_classes output:", predicted_classes)

        # Create a DataFrame for predictions
        predictions_df = spark.createDataFrame(predicted_classes.tolist(), IntegerType()).toDF("Prediction")
        
        # Append predictions to the original DataFrame
        batch_with_predictions = batch_df.withColumn("id", F.monotonically_increasing_id()).join(
            predictions_df.withColumn("id", F.monotonically_increasing_id()), on="id"
        ).drop("id")
        # print("batch_with_predictions output:", batch_with_predictions)

        # Write predictions to InfluxDB
        batch_with_predictions.foreach(write_to_influxdb)

# Write Predictions to InfluxDB
def write_to_influxdb(row):
    try:
        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            point = Point("IoMT") \
                .field("Prediction", int(row['Prediction'])) \
                .field("SrcBytes", int(row["SrcBytes"])) \
                .field("DstBytes", int(row["DstBytes"])) \
                .field("SrcLoad", float(row["SrcLoad"])) \
                .field("DstLoad", float(row["DstLoad"])) \
                .field("Temp", float(row["Temp"])) \
                .field("SpO2", int(row["SpO2"])) \
                .field("Pulse_Rate", int(row["Pulse_Rate"])) \
                .field("SYS", int(row["SYS"])) \
                .field("DIA", int(row["DIA"])) \
                .field("Heart_rate", int(row["Heart_rate"]))
            write_api.write(bucket=INFLUXDB_BUCKET, record=point)
    except Exception as e:
        print(f"Failed to write row to InfluxDB: {row}, Error: {e}")

# Stream Processing and Prediction
query = feature_df.writeStream \
    .foreachBatch(predict_stream) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()