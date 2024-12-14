from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, pandas_udf, PandasUDFType
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.models import load_model
import pandas as pd
import tensorflow as tf
import numpy as np
import os

# Create Spark session
spark = SparkSession.builder \
    .appName("SWaT Anomaly Detection with LSTM") \
    .getOrCreate()

# Ensure TensorFlow is available in the Spark environment
# spark.sparkContext.addPyFile("path/to/tensorflow.zip")  # Optional, depends on your setup

# Kafka configurations
KAFKA_BROKER = "kafka:9093"
INPUT_TOPIC = "industry_logs"
OUTPUT_TOPIC = "processed_swat_logs"

# Spark details
script_folder = os.path.dirname(os.path.abspath(__file__))
CHECKPOINT_PATH = os.path.join(script_folder, 'checkpoint')

# Schema for the SWaT dataset
schema = StructType([
    StructField("FIT101", DoubleType(), True),
    StructField("LIT101", DoubleType(), True),
    StructField("AIT201", DoubleType(), True),
    StructField("AIT202", DoubleType(), True),
    StructField("AIT203", DoubleType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("Normal/Attack", StringType(), True)
])

# Map for the custom optimizer
custom_objects = {
    # 'Adam': lambda **kwargs: Adam(learning_rate=kwargs.get('learning_rate', 0.001)),
    'Custom>Adam': lambda **kwargs: Adam(learning_rate=kwargs.get('learning_rate', 0.001))  # Map Custom>Adam to Adam
}

# Load the pre-trained TensorFlow model
model = tf.keras.models.load_model('/app/spark-scripts/swat_lstm_model.h5', custom_objects=custom_objects)
model.compile(optimizer='adam', loss='mse')  # Replace custom optimizer
model.save('swat_lstm_model_clean.h5')

def simple_predict(data):
    return [1 if x > 0.5 else 0 for x in data] 

@pandas_udf("integer", PandasUDFType.SCALAR)
def predict_udf(feature_series):
    return pd.Series(simple_predict(feature_series.tolist()))

# @pandas_udf("double", PandasUDFType.SCALAR)
# def predict_udf(feature_series):
#     # Assume model expects a certain shape, preprocess accordingly
#     features = np.array(feature_series.tolist())
#     features = features.reshape(features.shape[0], -1) 
#     predictions = model.predict(features)
#     return pd.Series(predictions.flatten())

# Read stream from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON payload
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Adding a debug message to indicate the streaming job is starting
print("Starting the streaming job... Listening on Kafka topic:", INPUT_TOPIC)

# Apply predictions
parsed_stream = parsed_stream.withColumn("anomaly_score", predict_udf(
    col("FIT101"), col("LIT101"), col("AIT201"), col("AIT202"), col("AIT203")
))
# Write results back to Kafka
output_stream = parsed_stream.selectExpr(
    "to_json(struct(*)) AS value"
)

output_stream.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", OUTPUT_TOPIC) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start() \
    .awaitTermination()

# Adding a message to indicate the streaming job has stopped
print("Streaming job stopped. Either the job has completed or an error occurred.")
