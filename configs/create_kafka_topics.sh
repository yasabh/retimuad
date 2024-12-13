#!/bin/bash

KAFKA_BROKER="kafka:9093"
TOPICS=("medical_logs" "industry_logs" "test_logs" "processed_swat_logs")

# Wait for Kafka to be ready
until /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $KAFKA_BROKER --list &>/dev/null; do
    echo "Waiting for Kafka to be ready..."
    sleep 5
done
echo "Kafka is ready."


# Create Kafka topics
echo "Creating Kafka topics..."
for TOPIC in "${TOPICS[@]}"; do
    if /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $KAFKA_BROKER --list | grep -wq $TOPIC; then
        echo "Topic '$TOPIC' already exists."
    else
        if /opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server $KAFKA_BROKER --replication-factor 1 --partitions 20 --topic $TOPIC; then
            echo "Topic '$TOPIC' created successfully."
        else
            echo "Error creating topic '$TOPIC'."
        fi
    fi
done
