from kafka import KafkaProducer
import csv
import json
import os
from tqdm import tqdm

# Kafka configuration
KAFKA_BROKER = "kafka:9093"  
KAFKA_TOPIC = "industry_logs"        

# File paths
DATASET_FILE = "./dataset/dataset.csv" 

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def stream_data_to_kafka(file_path, topic, batch_size=900):
    """
    Reads a CSV file and streams its rows to a Kafka topic in batches.

    Args:
        file_path (str): Path to the CSV file.
        topic (str): Kafka topic name.
        batch_size (int): Number of rows to send in a single batch.
    """
    print(f"Streaming data from {file_path} to Kafka topic '{topic}' in batches of {batch_size}...")

    try:
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            total_rows = sum(1 for _ in open(file_path, "r")) - 1  # Total rows excluding header
            file.seek(0)  # Reset file pointer after counting lines
            next(reader)  # Skip header row

            batch = []
            for row in tqdm(reader, total=total_rows, desc="Streaming rows", unit="row"):
                batch.append(row)
                if len(batch) >= batch_size:
                    # Send the batch as a single Kafka message
                    producer.send(topic, value=batch)
                    batch = []  # Clear the batch

            # Send remaining rows in the batch
            if batch:
                producer.send(topic, value=batch)

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
