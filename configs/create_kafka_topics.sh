#!/bin/bash

KAFKA_BROKER="kafka:9092"  # Use the internal Kafka network address within Docker
TOPICS=("images" "logs" "audio")

for TOPIC in "${TOPICS[@]}"; do
    # Check if the topic already exists
    if kafka-topics --bootstrap-server $KAFKA_BROKER --list | grep -wq $TOPIC; then
        echo "Topic '$TOPIC' already exists."
    else
        # Attempt to create the topic
        if kafka-topics --create --bootstrap-server $KAFKA_BROKER --replication-factor 1 --partitions 1 --topic $TOPIC; then
            echo "Topic '$TOPIC' created successfully."
        else
            echo "Error: Failed to create topic '$TOPIC'. Please check Kafka broker status and configuration."
        fi
    fi
done
