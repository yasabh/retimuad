from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import CommitFailedError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS
from tqdm import tqdm
import json

# Kafka configuration
KAFKA_BROKER = "localhost:9092"  # Adjust as per your setup
KAFKA_TOPIC = "industry_logs"    # The topic we are streaming from
CONSUMER_GROUP = "stream_processor_group"

# InfluxDB configuration
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "9FHwsLQM55crvf1CSV1mSGXr7TJF1slRhwDL7MvD1ZVcBb7ct5gx1aMFQiEb_QTHdsXtm4sJFa94GG6ab4nKzw=="
INFLUXDB_ORG = "example-org"
INFLUXDB_BUCKET = "industry_data"

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
        mv101 = float(data.get("MV101", 0))
        lit101 = float(data.get("LIT101", 0))
        normal_attack = data.get("Normal/Attack", "Unknown")

        # Prepare the data point for InfluxDB
        point = (
            Point("industry_logs")
            .field("MV101", mv101)
            .field("LIT101", lit101)
            .field("Normal_Attack", normal_attack)
        )

        # Write data to InfluxDB
        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            print(f"Written to InfluxDB: {data}")
        except Exception as e:
            print(f"Error writing to InfluxDB: {e}")
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