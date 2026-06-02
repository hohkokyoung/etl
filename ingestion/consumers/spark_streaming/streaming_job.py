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
SILVER_BUCKET = os.environ.get("S3_BUCKET_SILVER", "processed-silver")
SILVER_PATH = f"s3a://{SILVER_BUCKET}"
CHECKPOINT_PATH = f"{SILVER_PATH}/_checkpoints"
# Catalog name must match bronze_to_silver / silver_to_gold (hadoop catalog → lake.default.*)
CATALOG = "lake"

# ── Schemas per topic ─────────────────────────────────────────────

# Broad schema covering all three event types in the transactions topic.
# Fields not present in a given event type will be NULL after parsing.
TRANSACTION_SCHEMA = StructType([
    # order_created fields
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DecimalType(10, 2)),
    StructField("total_amount", DecimalType(12, 2)),
    StructField("status", StringType()),
    StructField("payment_method", StringType()),
    # customer_registered fields
    StructField("email", StringType()),
    StructField("name", StringType()),
    StructField("region", StringType()),
    StructField("age_group", StringType()),
    # product_created fields
    StructField("category", StringType()),
    StructField("price", DecimalType(10, 2)),
    StructField("stock_quantity", IntegerType()),
    # common — customer/product use "created_at" instead of "event_ts"
    StructField("event_ts", TimestampType()),
    StructField("created_at", TimestampType()),
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
    # transactions is handled separately via stream_transactions() below
    "sensors": SENSOR_SCHEMA,
    "financial": FINANCIAL_SCHEMA,
    "social": SOCIAL_SCHEMA,
}


def build_spark() -> SparkSession:
    master = os.environ.get("SPARK_MASTER", "local[2]")
    return (
        SparkSession.builder
        .master(master)
        .appName("etl-silver-streaming")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Use the same hadoop catalog as the batch jobs (lake → s3a://processed-silver)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", SILVER_PATH)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def stream_transactions(spark: SparkSession):
    """
    Read the transactions topic once and route to three silver tables by event_type:
      order_created        → silver_transactions
      customer_registered  → silver_customers
      product_created      → silver_products
    Uses foreachBatch so Kafka is consumed only once.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "transactions")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .select(
            F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("d"),
        )
        .select("d.*")
        # Normalise timestamp: orders use event_ts, customers/products use created_at
        .withColumn("_ts", F.coalesce(F.col("event_ts"), F.col("created_at")))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    def write_batch(batch_df, batch_id):
        # ── orders ──────────────────────────────────────────────────
        orders = batch_df.filter(F.col("event_type") == "order_created") \
                         .filter(F.col("_ts").isNotNull())
        if not orders.isEmpty():
            (orders.select(
                "order_id", "customer_id", "product_id", "quantity",
                "unit_price", "total_amount", "status", "payment_method",
                F.col("_ts").alias("event_ts"), "event_type", "source", "_ingested_at",
            ).writeTo(f"lake.default.silver_transactions").append())

        # ── customers ───────────────────────────────────────────────
        customers = batch_df.filter(F.col("event_type") == "customer_registered") \
                            .filter(F.col("customer_id").isNotNull())
        if not customers.isEmpty():
            (customers.select(
                "customer_id", "email", "name", "region", "age_group",
                F.col("_ts").alias("event_ts"), "source", "_ingested_at",
            ).writeTo(f"lake.default.silver_customers").append())

        # ── products ────────────────────────────────────────────────
        products = batch_df.filter(F.col("event_type") == "product_created") \
                           .filter(F.col("product_id").isNotNull())
        if not products.isEmpty():
            (products.select(
                "product_id", "name", "category", "price", "stock_quantity",
                F.col("_ts").alias("event_ts"), "source", "_ingested_at",
            ).writeTo(f"lake.default.silver_products").append())

    return (
        parsed.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/transactions")
        .trigger(processingTime="30 seconds")
        .start()
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
        .option("path", f"lake.default.silver_{topic}")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Create Iceberg silver tables for sensors / financial / social
    for topic, schema in TOPIC_SCHEMAS.items():
        cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS lake.default.silver_{topic} (
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

    # Create the three tables that come from the transactions topic
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS lake.default.silver_transactions (
            order_id STRING, customer_id STRING, product_id STRING,
            quantity INT, unit_price DECIMAL(10,2), total_amount DECIMAL(12,2),
            status STRING, payment_method STRING,
            event_ts TIMESTAMP, event_type STRING, source STRING, _ingested_at TIMESTAMP
        ) USING iceberg LOCATION '{SILVER_PATH}/transactions'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS lake.default.silver_customers (
            customer_id STRING, email STRING, name STRING,
            region STRING, age_group STRING,
            event_ts TIMESTAMP, source STRING, _ingested_at TIMESTAMP
        ) USING iceberg LOCATION '{SILVER_PATH}/customers'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS lake.default.silver_products (
            product_id STRING, name STRING, category STRING,
            price DECIMAL(10,2), stock_quantity INT,
            event_ts TIMESTAMP, source STRING, _ingested_at TIMESTAMP
        ) USING iceberg LOCATION '{SILVER_PATH}/products'
    """)

    queries = [stream_topic(spark, topic, schema) for topic, schema in TOPIC_SCHEMAS.items()]
    queries.append(stream_transactions(spark))

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
