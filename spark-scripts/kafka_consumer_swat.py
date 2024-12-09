from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import CommitFailedError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WriteOptions
import os
import json

# Kafka configuration
KAFKA_BROKER = "kafka:9093"  # Adjust as per your setup
KAFKA_TOPIC = "industry_logs"    # The topic we are streaming from
CONSUMER_GROUP = "stream_processor_group"

# InfluxDB configuration
# Read config from environment variables
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "stream-org")
INFLUXDB_BUCKET1 = os.getenv("INFLUXDB_BUCKET1", "industry_data")
# INFLUXDB_BUCKET2 = os.getenv("INFLUXDB_BUCKET2", "default_bucket")
# INFLUXDB_BUCKET3 = os.getenv("INFLUXDB_BUCKET3", "default_bucket")

# Batch size for InfluxDB writes
BATCH_SIZE = 200  

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
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=BATCH_SIZE, flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000))

print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")

try:
    for message in consumer:
        data = message.value

        # Debug: Print the incoming data
        print(f"Received message: {data}")

        # Ensure the data is a list of dictionaries
        if isinstance(data, list):
            for record in data:
                if isinstance(record, dict):
                    lit101 = float(record.get("LIT101", 0))
                    lit301 = float(record.get("LIT301", 0))
                    lit401 = float(record.get("LIT401", 0))
                    fit101 = float(record.get("FIT101", 0))
                    normal_attack = record.get("Normal/Attack", "Unknown")

                    # Prepare the data point for InfluxDB
                    point = (
                        Point("industry_logs")
                        .field("LIT101", lit101)
                        .field("LIT301", lit301)
                        .field("LIT401", lit401)
                        .field("FIT101", fit101)
                        .field("Normal_Attack", normal_attack)
                    )

                    # Write data to InfluxDB
                    try:
                        write_api.write(bucket=INFLUXDB_BUCKET1, org=INFLUXDB_ORG, record=point)
                        print(f"Written to InfluxDB: {record}")
                    except Exception as e:
                        print(f"Error writing to InfluxDB: {e}")
                        continue  # Skip committing if writing fails
                else:
                    print(f"Unexpected record format: {record}")
        else:
            print(f"Unexpected data format: {data}")

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