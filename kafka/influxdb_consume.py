from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import CommitFailedError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS
from tqdm import tqdm
import json
import os

# Kafka configuration
KAFKA_BROKER = "kafka:9093"

# InfluxDB configuration
INFLUXDB_URL = "http://localhost:8086"

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
DATASET_FIELDS = os.getenv("DATASET_FIELDS", "[]")
CONSUMER_GROUP = KAFKA_TOPIC + "_influxdb_group"
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Batch size for InfluxDB writes
BATCH_SIZE = 500  # Number of rows to write in a single batch

# Initialize Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=False,  # Disable auto-commit for manual control
    group_id=CONSUMER_GROUP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Initialize InfluxDB client
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=ASYNCHRONOUS)

print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")

try:
    for message in consumer:
        data = message.value

        # Process the message
        fields_data = {}
        for field, default_value in DATASET_FIELDS:
            fields_data[field] = data.get(field, default_value)

        # Prepare the data point for InfluxDB
        point = Point(KAFKA_TOPIC)
        for field, value in fields_data.items():
            point = point.field(field, value)

        # Write data to InfluxDB
        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            # print(f"Written to InfluxDB: {data}")
        except Exception as e:
            # print(f"Error writing to InfluxDB: {e}")
            continue  # Skip committing if writing fails

        # Commit the offset after successful processing
        try:
            consumer.commit({
                TopicPartition(message.topic, message.partition): OffsetAndMetadata(message.offset + 1, None)
            })
        except CommitFailedError as e:
            print(f"Offset commit failed: {e}")
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()
    influx_client.close()