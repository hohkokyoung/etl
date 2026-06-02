FROM apache/airflow:2.9.3
USER root

# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless wget && \
    rm -rf /var/lib/apt/lists/*

# Download S3 + Iceberg JARs for PySpark to use
RUN mkdir -p /opt/spark-jars && \
    wget -q -O /opt/spark-jars/hadoop-aws-3.3.4.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    wget -q -O /opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" && \
    wget -q -O /opt/spark-jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.0/iceberg-spark-runtime-3.5_2.12-1.6.0.jar"

USER airflow

# Install PySpark + ClickHouse client + dbt
RUN pip install --no-cache-dir pyspark==3.5.3 clickhouse-connect==0.7.16 dbt-clickhouse==1.8.0
