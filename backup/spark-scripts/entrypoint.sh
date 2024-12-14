#!/bin/bash

# Run only for spark-app
if [ "$SPARK_APP_MODE" = "true" ]; then
  while [ ! -f /app/shared/producer_finished.flag ]; do
    echo 'Waiting for producer to finish...'
    sleep 5
  done

  # Check and set up Ivy cache
  if [ ! -d "/tmp/ivy/cache" ]; then
    mkdir -p /tmp/ivy/cache
    chmod -R 777 /tmp/ivy
    echo "Ivy cache directory created and permissions set."
  fi

  # Debugging outputs
  echo "Environment Variables:"
  env

  # Debugging the directory structure
  echo "Checking Ivy Cache Directory:"
  ls -ld /tmp/ivy/cache

  exec spark-submit --master spark://spark-master:7077 \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 \
       --conf spark.hadoop.security.authentication=none \
       --conf spark.hadoop.security.authorization=false \
       --conf spark.hadoop.fs.defaultFS=file:/// \
       /app/spark_streaming_swat.py
else
  # Default behavior for other containers (spark-master, spark-worker)
  echo "Running default entrypoint for $SPARK_MODE"
  exec "/opt/bitnami/scripts/spark/run.sh"
fi
