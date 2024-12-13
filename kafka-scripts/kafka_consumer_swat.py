from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import CommitFailedError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WriteOptions
import os
import json
from tqdm import tqdm  # For the progress bar

# Kafka configuration
KAFKA_BROKER = "kafka:9093"  # Adjust as per your setup
KAFKA_TOPIC = "processed_swat_logs"  # The topic we are streaming from
CONSUMER_GROUP = "stream_processor_group"

# InfluxDB configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "stream-org")
INFLUXDB_BUCKET1 = os.getenv("INFLUXDB_BUCKET1", "industry_data")

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

# Batch to hold records
batch = []
progress_bar = tqdm(total=0, unit="record", desc="Processed")  # Progress bar

try:
    for message in consumer:
        data = message.value

        # Ensure the data is a list of dictionaries
        if isinstance(data, list):
            for record in data:
                if isinstance(record, dict):
                    fit101 = float(record.get("FIT101", 0))
                    lit101 = float(record.get("LIT101", 0))
                    ait101 = float(record.get("AIT201", 0))
                    ait102 = float(record.get("AIT202", 0))
                    ait103 = float(record.get("AIT203", 0))
                    normal_attack = record.get("Normal/Attack", "Unknown")

                    # Prepare the data point for InfluxDB
                    point = (
                        Point("processed_swat_logs")
                        .field("FIT101", fit101)
                        .field("LIT101", lit101)
                        .field("AIT201", ait101)
                        .field("AIT202", ait102)
                        .field("AIT203", ait103)
                        .field("Normal_Attack", normal_attack)
                    )

                    # Add the point to the batch
                    batch.append(point)

                    # If the batch size is reached, write to InfluxDB
                    if len(batch) >= BATCH_SIZE:
                        try:
                            write_api.write(bucket=INFLUXDB_BUCKET1, org=INFLUXDB_ORG, record=batch)
                            progress_bar.total += len(batch)
                            progress_bar.update(len(batch))
                            print(f"Successfully written {len(batch)} records to InfluxDB.")
                            batch = []  # Clear the batch
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
    # Write any remaining records in the batch
    if batch:
        try:
            write_api.write(bucket=INFLUXDB_BUCKET1, org=INFLUXDB_ORG, record=batch)
            progress_bar.total += len(batch)
            progress_bar.update(len(batch))
            print(f"Successfully written remaining {len(batch)} records to InfluxDB.")
        except Exception as e:
            print(f"Error writing remaining records to InfluxDB: {e}")

    consumer.close()
    influx_client.close()
    progress_bar.close()
