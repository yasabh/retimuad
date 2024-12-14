from kafka import KafkaProducer
import csv
import json
import time
import os
from tqdm import tqdm

KAFKA_BROKER = "kafka:9093"
DATASET_FILE = "/Datasets/" + os.getenv("DATASET_FILE")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def stream_data_to_kafka(file_path, topic):
    """
    Reads a CSV file and streams its rows to a Kafka topic.

    Args:
        file_path (str): Path to the CSV file.
        topic (str): Kafka topic name.
    """
    print(f"Streaming data from {file_path} to Kafka topic '{topic}'...")

    try:
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            total_rows = sum(1 for _ in open(file_path, "r")) - 1  # Total rows excluding header
            file.seek(0)  # Reset file pointer after counting lines
            next(reader)  # Skip header row

            # Stream each row with a progress bar
            for row in tqdm(reader, total=total_rows, desc="Streaming rows", unit="row"):
                # Send each row as a Kafka message
                producer.send(topic, value=row)
                time.sleep(0.01)  # Simulate real-time streaming (adjust as needed)
    except Exception as e:
        print(f"Error while streaming data: {e}")
    finally:
        producer.flush()
        print("Data streaming completed.")

if __name__ == "__main__":
    # Ensure the dataset file exists
    if not os.path.exists(DATASET_FILE):
        print(f"Dataset file not found: {DATASET_FILE}")
    else:
        # Stream data from the dataset file to Kafka
        stream_data_to_kafka(DATASET_FILE, KAFKA_TOPIC)
