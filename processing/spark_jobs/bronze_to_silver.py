"""
Spark batch job: bronze → silver
Reads raw Parquet from S3 bronze, applies schema enforcement, deduplication,
and writes Iceberg tables to S3 silver.

spark-submit bronze_to_silver.py --source transactions --date 2024-11-01
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, FloatType, IntegerType, StringType, StructField,
    StructType, TimestampType, BooleanType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BRONZE = os.environ.get("S3_BUCKET_BRONZE", "raw-bronze")
S3_SILVER = os.environ.get("S3_BUCKET_SILVER", "processed-silver")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")

SCHEMAS: dict[str, StructType] = {
    "transactions": StructType([
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
    ]),
    "sensors": StructType([
        StructField("reading_id", StringType()),
        StructField("location_id", StringType()),
        StructField("site_name", StringType()),
        StructField("temperature", FloatType()),
        StructField("pressure", FloatType()),
        StructField("humidity", FloatType()),
        StructField("vibration", FloatType()),
        StructField("lat", FloatType()),
        StructField("lon", FloatType()),
        StructField("is_anomaly", BooleanType()),
        StructField("battery_pct", FloatType()),
        StructField("event_ts", TimestampType()),
    ]),
    "financial": StructType([
        StructField("trade_id", StringType()),
        StructField("asset", StringType()),
        StructField("side", StringType()),
        StructField("quantity", FloatType()),
        StructField("price", DecimalType(18, 8)),
        StructField("total_value", DecimalType(18, 4)),
        StructField("exchange", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("event_type", StringType()),
    ]),
    "social": StructType([
        StructField("post_id", StringType()),
        StructField("user_id", StringType()),
        StructField("platform", StringType()),
        StructField("content_type", StringType()),
        StructField("topic", StringType()),
        StructField("likes", IntegerType()),
        StructField("shares", IntegerType()),
        StructField("comments", IntegerType()),
        StructField("sentiment", StringType()),
        StructField("engagement_score", FloatType()),
        StructField("is_sponsored", BooleanType()),
        StructField("event_ts", TimestampType()),
    ]),
}

# Primary key per source (for deduplication)
DEDUP_KEYS: dict[str, str] = {
    "transactions": "order_id",
    "sensors": "reading_id",
    "financial": "trade_id",
    "social": "post_id",
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("etl-bronze-to-silver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", f"s3a://{S3_SILVER}")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, source: str, date: str) -> DataFrame:
    path = f"s3a://{S3_BRONZE}/source={source}/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/"
    logger.info("Reading bronze: %s", path)
    # Bronze is all-string schema; cast to typed below
    return spark.read.parquet(path)


def cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    typed_cols = []
    bronze_cols = {c.lower() for c in df.columns}
    for field in schema.fields:
        if field.name.lower() in bronze_cols:
            typed_cols.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            typed_cols.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(typed_cols)


def deduplicate(df: DataFrame, key: str) -> DataFrame:
    from pyspark.sql.window import Window
    w = Window.partitionBy(key).orderBy(F.col("event_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def add_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df
        .filter(F.col("event_ts").isNotNull())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_year", F.year("event_ts"))
        .withColumn("_month", F.month("event_ts"))
        .withColumn("_day", F.dayofmonth("event_ts"))
    )


def write_silver(df: DataFrame, source: str, spark: SparkSession):
    table = f"lake.default.silver_{source}"
    # Create table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING iceberg
        PARTITIONED BY (_year, _month, _day)
        LOCATION 's3a://{S3_SILVER}/{source}'
        AS SELECT * FROM (SELECT * FROM {table} LIMIT 0) t
    """) if False else None  # skip — use append mode which auto-creates

    df.writeTo(table).using("iceberg").partitionedBy("_year", "_month", "_day").append()
    logger.info("Written %d rows to %s", df.count(), table)


def process(source: str, date: str):
    if source not in SCHEMAS:
        raise ValueError(f"Unknown source: {source}. Valid: {list(SCHEMAS)}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    batch_id = f"{source}_{date}_{datetime.now(timezone.utc).strftime('%H%M%S')}"

    raw = read_bronze(spark, source, date)
    typed = cast_to_schema(raw, SCHEMAS[source])
    deduped = deduplicate(typed, DEDUP_KEYS[source])
    final = add_metadata(deduped, batch_id)

    write_silver(final, source, spark)
    logger.info("bronze→silver complete: source=%s date=%s rows=%d", source, date, final.count())
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, choices=list(SCHEMAS))
    parser.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    args = parser.parse_args()
    process(args.source, args.date)
