#!/bin/bash

KAFKA_BROKER="kafka:9092"
ZOOKEEPER_HOST="zookeeper:2181"
TOPICS=("images" "logs" "audio")

# Check if any broker is registered with Zookeeper
BROKER_COUNT=$(echo "ls /brokers/ids" | zkCli.sh -server $ZOOKEEPER_HOST 2>/dev/null | grep -o '[0-9]' | wc -l)

if [[ "$BROKER_COUNT" -eq 0 ]]; then
    echo "No brokers are connected to Zookeeper. Exiting..."
    exit 1
else
    echo "$BROKER_COUNT broker(s) connected to Zookeeper."
fi

# Proceed to create topics if broker(s) exist
for TOPIC in "${TOPICS[@]}"; do
    if /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $KAFKA_BROKER --list | grep -wq $TOPIC; then
        echo "Topic '$TOPIC' already exists."
    else
        if /opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server $KAFKA_BROKER --replication-factor 1 --partitions 1 --topic $TOPIC; then
            echo "Topic '$TOPIC' created successfully."
        else
            echo "Error creating topic '$TOPIC'."
        fi
    fi
done
