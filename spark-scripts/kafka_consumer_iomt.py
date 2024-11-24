from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import CommitFailedError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS
from tqdm import tqdm
import json

# Kafka configuration
KAFKA_BROKER = "localhost:9092"  # Adjust as per your setup
KAFKA_TOPIC = "iomt"    # The topic we are streaming from
CONSUMER_GROUP = "stream_iomt_group"

# InfluxDB configuration
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "0c8l3cCg8YubsPeSwqEOuDLyXmXukIPqtebNEWTIwRM8Uk_cD9gv7rpMqA8JM6dlGqte9IV7xsjUxITzrte0TQ=="
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
        dir = data.get("Dir", 0)
        flgs = data.get("Flgs", 0)
        sport = int(data.get("Sport", 0))

        # Prepare the data point for InfluxDB
        point = (
            Point("iomt_data")
            .field("Dir", dir)
            .field("Flgs", flgs)
            .field("Sport", sport)
        )

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