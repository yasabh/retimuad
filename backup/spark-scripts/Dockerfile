# Base Spark image
FROM bitnami/spark:2.4.4

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
USER root
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV SPARK_USER=nonexistentuser
RUN pip install --no-cache-dir tensorflow-cpu==2.4.1
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir -r requirements.txt

# Add minimal Hadoop configuration
RUN mkdir -p /opt/hadoop/etc/hadoop
COPY core-site.xml /opt/hadoop/etc/hadoop/
COPY hdfs-site.xml /opt/hadoop/etc/hadoop/

# Copy application and dependencies
COPY spark_streaming_swat.py /app/spark_streaming_swat.py
COPY log4j.properties /opt/bitnami/spark/conf/

# Configure Ivy resolution directory
RUN mkdir -p /tmp/ivy/cache && chmod -R 777 /tmp/ivy

# Debug: List the directory to confirm creation
RUN ls -ld /tmp/ivy && ls -ld /tmp/ivy/cache

ENV IVY_HOME=/tmp/ivy
ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf
RUN echo "spark.jars.ivy=/tmp/ivy" >> $SPARK_CONF_DIR/spark-defaults.conf

# Install wget for downloading Kafka connectors
RUN apt-get update && apt-get install -y wget && apt-get clean
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.4.4/spark-sql-kafka-0-10_2.11-2.4.4.jar -P /opt/bitnami/spark/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.4.0/kafka-clients-2.4.0.jar -P /opt/bitnami/spark/jars/

COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

# Switch back to non-root user
USER 1001

# Entrypoint for Spark Streaming
ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]
