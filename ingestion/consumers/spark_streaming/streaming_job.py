"""
Spark Structured Streaming consumer — reads Kafka, applies schema+typing,
writes Iceberg tables to S3 silver bucket (processed-silver/).

Run via:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
    org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,\
    software.amazon.awssdk:bundle:2.26.0 \
    streaming_job.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType
)

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
SILVER_PATH = "s3a://processed-silver"
CHECKPOINT_PATH = "s3a://processed-silver/_checkpoints"

# ── Schemas per topic ─────────────────────────────────────────────

TRANSACTION_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DecimalType(10, 2)),
    StructField("total_amount", DecimalType(12, 2)),
    StructField("status", StringType()),
    StructField("payment_method", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("event_type", StringType()),
    StructField("source", StringType()),
])

SENSOR_SCHEMA = StructType([
    StructField("reading_id", StringType()),
    StructField("location_id", StringType()),
    StructField("site_name", StringType()),
    StructField("temperature", FloatType()),
    StructField("pressure", FloatType()),
    StructField("humidity", FloatType()),
    StructField("is_anomaly", StringType()),
    StructField("battery_pct", FloatType()),
    StructField("event_ts", TimestampType()),
    StructField("source", StringType()),
])

FINANCIAL_SCHEMA = StructType([
    StructField("trade_id", StringType()),
    StructField("asset", StringType()),
    StructField("side", StringType()),
    StructField("quantity", FloatType()),
    StructField("price", DecimalType(18, 8)),
    StructField("total_value", DecimalType(18, 4)),
    StructField("exchange", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("source", StringType()),
])

SOCIAL_SCHEMA = StructType([
    StructField("post_id", StringType()),
    StructField("user_id", StringType()),
    StructField("platform", StringType()),
    StructField("content_type", StringType()),
    StructField("topic", StringType()),
    StructField("likes", IntegerType()),
    StructField("shares", IntegerType()),
    StructField("comments", IntegerType()),
    StructField("sentiment", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("source", StringType()),
])

TOPIC_SCHEMAS = {
    "transactions": TRANSACTION_SCHEMA,
    "sensors": SENSOR_SCHEMA,
    "financial": FINANCIAL_SCHEMA,
    "social": SOCIAL_SCHEMA,
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("etl-silver-streaming")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.type", "glue")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", SILVER_PATH)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def stream_topic(spark: SparkSession, topic: str, schema: StructType):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"), "offset", "partition", "timestamp")
        .select("data.*", F.col("timestamp").alias("_kafka_ts"), "offset", "partition")
        .filter(F.col("event_ts").isNotNull())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_year", F.year("event_ts"))
        .withColumn("_month", F.month("event_ts"))
        .withColumn("_day", F.dayofmonth("event_ts"))
    )

    return (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", f"glue_catalog.etl_lake.silver_{topic}")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Create Iceberg tables if they don't exist
    for topic, schema in TOPIC_SCHEMAS.items():
        cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.etl_lake.silver_{topic} (
                {cols},
                _kafka_ts TIMESTAMP,
                offset BIGINT,
                partition INT,
                _ingested_at TIMESTAMP,
                _year INT,
                _month INT,
                _day INT
            )
            USING iceberg
            PARTITIONED BY (_year, _month, _day)
            LOCATION '{SILVER_PATH}/{topic}'
        """)

    queries = [stream_topic(spark, topic, schema) for topic, schema in TOPIC_SCHEMAS.items()]

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
