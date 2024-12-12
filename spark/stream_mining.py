import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Kafka and InfluxDB configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IoMT_Stream_Mining") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Define schema and extract JSON fields
schema = "Flgs object,Sport object,SrcBytes INT,DstBytes INT,SrcLoad FLOAT,DstLoad FLOAT,SIntPkt FLOAT,DIntPkt FLOAT,SIntPktAct FLOAT,SrcJitter FLOAT,DstJitter FLOAT,sMaxPktSz INT,dMaxPktSz INT,sMinPktSz INT,Dur FLOAT,TotPkts INT,TotBytes INT,Load FLOAT,Loss INT,pLoss FLOAT,pSrcLoss FLOAT,pDstLoss FLOAT,Rate FLOAT,SrcMac object,Packet_num INT,Temp FLOAT,SpO2 INT,Pulse_Rate INT,SYS INT,DIA INT, Heart_rate INT, Resp_Rate INT, ST FLOAT, Attack Category object, Label INT"
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Assemble features
assembler = VectorAssembler(inputCols=["SrcBytes", "DstBytes", "SrcLoad", "DstLoad", "Temp", "SpO2", "Pulse_Rate", "SYS", "DIA", "Heart_rate"], outputCol="features")
feature_df = assembler.transform(parsed_df)

# Apply Ridge Regression
lr = LinearRegression(featuresCol="features", labelCol="Label", maxIter=10, regParam=0.1, elasticNetParam=0.8)
model = lr.fit(feature_df)

# Write results to InfluxDB
def write_to_influxdb(row):
    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org="org") as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        point = Point("IoMT") \
            .field("prediction", row["prediction"]) \
            .field("label", row["Label"])
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)

query = model.transform(feature_df) \
    .select("features", "Label", "prediction") \
    .writeStream \
    .foreach(write_to_influxdb) \
    .start()

query.awaitTermination()