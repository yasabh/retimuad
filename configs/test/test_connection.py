from kafka import KafkaProducer
import json

# Kafka broker address
KAFKA_BROKER = "localhost:9092"  # Update this if running inside Docker

# Kafka topic to send messages to
TOPIC = "test_logs"  # Replace with your desired topic name

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # Serialize data to JSON
    acks='all'  # Ensure broker acknowledges the message
)

# Callback function for successful or failed delivery
def on_send_success(record_metadata):
    print(f"Message delivered to {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")

def on_send_error(ex):
    print(f"Error sending message: {ex}")

# Function to send messages
def send_message(message):
    try:
        producer.send(TOPIC, value=message).add_callback(on_send_success).add_errback(on_send_error)
        producer.flush()  # Ensure the message is sent
    except Exception as e:
        print(f"Error sending message: {e}")

if __name__ == "__main__":
    # Example messages to send
    messages = [
        {"type": "log", "message": "This is the first log message"},
        {"type": "log", "message": "This is the second log message"}
    ]

    for msg in messages:
        send_message(msg)

    producer.close() 
