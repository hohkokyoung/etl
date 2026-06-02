FROM apache/spark:3.5.3
USER root

# Pre-download all required JARs so Spark jobs don't pull from Maven at runtime
# This runs once during image build — not on every job submission
RUN mkdir -p /opt/spark-jars && \
    # Iceberg runtime for Spark 3.5
    wget -q -O /opt/spark-jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.0/iceberg-spark-runtime-3.5_2.12-1.6.0.jar" && \
    # Hadoop AWS connector (S3A filesystem)
    wget -q -O /opt/spark-jars/hadoop-aws-3.3.4.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    # AWS SDK bundle (required by hadoop-aws for S3 access)
    wget -q -O /opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" && \
    # Kafka connector for Spark Structured Streaming
    wget -q -O /opt/spark-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar" && \
    # Kafka clients (required by spark-sql-kafka)
    wget -q -O /opt/spark-jars/kafka-clients-3.4.1.jar \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" && \
    # Spark token provider for Kafka
    wget -q -O /opt/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar" && \
    # Commons pool (required by Kafka connector)
    wget -q -O /opt/spark-jars/commons-pool2-2.11.1.jar \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" && \
    echo "All JARs downloaded successfully"
