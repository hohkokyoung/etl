"""
Spark batch job: silver → gold
Reads typed Iceberg tables from S3 silver, applies business aggregations,
and writes curated Iceberg marts to S3 gold.

spark-submit silver_to_gold.py --date 2024-11-01
"""
import argparse
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_SILVER = os.environ.get("S3_BUCKET_SILVER", "processed-silver")
S3_GOLD = os.environ.get("S3_BUCKET_GOLD", "curated-gold")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("etl-silver-to-gold")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hadoop")
        .config("spark.sql.catalog.silver.warehouse", f"s3a://{S3_SILVER}")
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gold.type", "hadoop")
        .config("spark.sql.catalog.gold.warehouse", f"s3a://{S3_GOLD}")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


def build_fact_sales(spark: SparkSession, date: str) -> DataFrame:
    """Joins transactions with dim stubs to produce fact_sales gold mart."""
    txn = spark.read.format("iceberg").load(f"silver.default.silver_transactions").filter(
        F.col("event_ts").cast("date") == date
    )
    return (
        txn
        .withColumn("date_key", F.date_format("event_ts", "yyyyMMdd").cast("int"))
        .withColumn("sale_id", F.expr("uuid()"))
        .select(
            "sale_id", "order_id", "customer_id", "product_id",
            "date_key", "quantity", "unit_price", "total_amount", "status",
            F.col("event_ts"), F.col("_batch_id"), F.current_timestamp().alias("_loaded_at"),
        )
    )


def build_fact_sensors(spark: SparkSession, date: str) -> DataFrame:
    sensors = spark.read.format("iceberg").load(f"silver.default.silver_sensors").filter(
        F.col("event_ts").cast("date") == date
    )
    return (
        sensors
        .withColumn("date_key", F.date_format("event_ts", "yyyyMMdd").cast("int"))
        .select(
            "reading_id", "location_id", "date_key",
            "temperature", "pressure", "humidity",
            "lat", "lon",
            F.col("event_ts"), F.current_timestamp().alias("_loaded_at"),
        )
    )


def build_fact_trades(spark: SparkSession, date: str) -> DataFrame:
    fin = spark.read.format("iceberg").load(f"silver.default.silver_financial").filter(
        F.col("event_ts").cast("date") == date
    )
    return (
        fin
        .withColumn("date_key", F.date_format("event_ts", "yyyyMMdd").cast("int"))
        .select(
            "trade_id", "asset", "side", "quantity", "price", "total_value",
            "date_key", F.col("event_ts"), F.current_timestamp().alias("_loaded_at"),
        )
    )


def build_fact_social(spark: SparkSession, date: str) -> DataFrame:
    social = spark.read.format("iceberg").load(f"silver.default.silver_social").filter(
        F.col("event_ts").cast("date") == date
    )
    return (
        social
        .withColumn("date_key", F.date_format("event_ts", "yyyyMMdd").cast("int"))
        .select(
            "post_id", "user_id", "platform", "content_type",
            "likes", "shares", "comments",
            "date_key", F.col("event_ts"), F.current_timestamp().alias("_loaded_at"),
        )
    )


def write_gold(df: DataFrame, table_name: str, spark: SparkSession):
    full_table = f"gold.default.{table_name}"
    df.writeTo(full_table).using("iceberg").partitionedBy("date_key").createOrReplace()
    logger.info("Written %s → gold", table_name)


def process(date: str):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    write_gold(build_fact_sales(spark, date), "fact_sales", spark)
    write_gold(build_fact_sensors(spark, date), "fact_sensor_readings", spark)
    write_gold(build_fact_trades(spark, date), "fact_trades", spark)
    write_gold(build_fact_social(spark, date), "fact_social_engagement", spark)

    logger.info("silver→gold complete for date=%s", date)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    args = parser.parse_args()
    process(args.date)
