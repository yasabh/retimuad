from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType, StringType
import numpy as np
import tensorflow as tf
import os

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("RealTimeDataProcessingWithCUSUM") \
    .getOrCreate()

# Load TensorFlow model dynamically based on feature name
def load_model(feature_name):
    model_path = f"{os.path.dirname(os.path.abspath(__file__))}/models/{feature_name}_swat_model.keras"
    return tf.keras.models.load_model(model_path)

models = {}  # Cache to store models

# Preprocess data and make predictions using TensorFlow model
def preprocess_and_predict(feature_name, value):
    if feature_name not in models:
        models[feature_name] = load_model(feature_name)
    model = models[feature_name]
    normalized_value = (value - 0.5) / 0.5
    reshaped_value = np.array([normalized_value]).reshape(1, 1, 1)
    prediction = model.predict(reshaped_value)[0][0]
    return float(prediction)

predict_udf = udf(preprocess_and_predict, FloatType())

# Define the schema for the incoming data
schema = StringType()

# Read streaming data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "sensor_topic") \
    .load() \
    .selectExpr("CAST(key AS STRING) as feature_name", "CAST(value AS STRING) as value")

# Apply the prediction UDF
predictions_df = df.withColumn("prediction", predict_udf(col("feature_name"), col("value")))

# CUSUM UDF to maintain state and detect anomalies
@udf(FloatType())
def cusum_udf(value, state):
    k = 0.5  # Sensitivity factor, adjust based on your requirements
    target = 0.0  # Target value for predictions, adjust as necessary
    if not state or 'cum_sum' not in state:
        state = {'cum_sum': 0.0}
    change = value - target
    cusum = state['cum_sum'] + (change - k if change > 0 else change + k)
    cusum = max(0, cusum)  # Reset CUSUM when it falls below zero
    state['cum_sum'] = cusum  # Update state
    return cusum

# Apply CUSUM calculations
cusum_df = predictions_df.withColumn("cusum", cusum_udf("prediction"))

# Write results to the console for demonstration purposes
# Replace this with writing to InfluxDB or another sink as necessary
query = cusum_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
