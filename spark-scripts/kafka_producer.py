from kafka import KafkaProducer
import csv
import json
import time
import os

# Kafka configuration
KAFKA_BROKER = "localhost:9093"  # Adjust if needed

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Flag directory to track loaded datasets
LOADED_DATASET_DIR = "loaded_datasets"
if not os.path.exists(LOADED_DATASET_DIR):
    os.makedirs(LOADED_DATASET_DIR)

# Load dataset and send each row as a message
def load_csv_to_kafka(file_path, topic):
    # Check if the dataset for this topic was already loaded
    loaded_flag_path = os.path.join(LOADED_DATASET_DIR, f"{topic}.loaded")
    if os.path.exists(loaded_flag_path):
        print(f"Dataset for topic '{topic}' already loaded. Skipping...")
        return

    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.send(topic, value=row)
            print(f"Sent to {topic}: {row}")
            time.sleep(1)  # Optional: add a delay to simulate real-time streaming

    # Mark dataset as loaded
    with open(loaded_flag_path, "w") as flag_file:
        flag_file.write("loaded")
    
    print(f"Dataset for topic '{topic}' loaded successfully.")

    # Flush and close the producer
    producer.flush()
    producer.close()

if __name__ == "__main__":
    # Load data for each topic and its corresponding dataset
    topics_datasets = {
        "images": "path/to/images_dataset.csv",
        "logs": "path/to/logs_dataset.csv",
        "audio": "path/to/audio_dataset.csv"
    }
    
    for topic, file_path in topics_datasets.items():
        load_csv_to_kafka(file_path, topic)
