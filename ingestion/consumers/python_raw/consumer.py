"""
Python Raw Consumer — reads all Kafka topics and writes raw JSON/Parquet to S3 bronze.
Batches messages and writes Parquet partitioned by source/date/hour.
"""
import io
import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TOPICS = ["transactions", "sensors", "financial", "social"]
FLUSH_INTERVAL_SECONDS = int(os.environ.get("FLUSH_INTERVAL_SECONDS", "30"))
BATCH_MAX_ROWS = int(os.environ.get("BATCH_MAX_ROWS", "5000"))
S3_BUCKET = os.environ.get("S3_BUCKET_BRONZE", "raw-bronze")

_running = True


def _handle_signal(sig, frame):
    global _running
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name="us-east-1",
    )


def make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id": "raw-bronze-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 30000,
    })


def flush_batch(s3, topic: str, rows: list[dict]) -> int:
    """Write a batch of dicts as Parquet to S3 bronze, partitioned by date/hour."""
    if not rows:
        return 0

    now = datetime.now(timezone.utc)
    key = (
        f"source={topic}/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"{now.strftime('%Y%m%d_%H%M%S')}_{len(rows)}.parquet"
    )

    # Convert to Arrow table — all values as strings for schema flexibility in bronze
    fields = sorted({k for row in rows for k in row.keys()})
    arrays = {f: [str(row.get(f, "")) for row in rows] for f in fields}
    table = pa.table({f: pa.array(arrays[f], type=pa.string()) for f in fields})

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.read())
    logger.info("Flushed %d rows → s3://%s/%s", len(rows), S3_BUCKET, key)
    return len(rows)


def run():
    s3 = make_s3_client()
    consumer = make_consumer()
    consumer.subscribe(TOPICS)

    batches: dict[str, list[dict]] = defaultdict(list)
    last_flush = time.time()
    total_written = 0

    logger.info("Python consumer started — subscribed to %s", TOPICS)

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                err = msg.error()
                # Topics don't exist yet (no producers running) — wait and retry
                if err.code() in (KafkaError.UNKNOWN_TOPIC_OR_PART,
                                  KafkaError._NO_OFFSET,
                                  KafkaError.UNKNOWN_MEMBER_ID):
                    logger.debug("Transient Kafka error (topics not ready yet): %s", err)
                else:
                    raise KafkaException(err)
            else:
                topic = msg.topic()
                try:
                    event = json.loads(msg.value())
                    event["_kafka_offset"] = msg.offset()
                    event["_kafka_partition"] = msg.partition()
                    event["_ingested_at"] = datetime.now(timezone.utc).isoformat()
                    batches[topic].append(event)
                except (json.JSONDecodeError, Exception) as e:
                    logger.warning("Parse error on %s offset %d: %s", topic, msg.offset(), e)

            # Flush when batch is large enough or time interval exceeded
            should_flush = (
                time.time() - last_flush >= FLUSH_INTERVAL_SECONDS
                or any(len(b) >= BATCH_MAX_ROWS for b in batches.values())
            )
            if should_flush:
                for topic, rows in batches.items():
                    if rows:
                        total_written += flush_batch(s3, topic, rows)
                batches.clear()
                try:
                    consumer.commit(asynchronous=False)
                except KafkaException:
                    pass  # No offsets yet (topics empty, no messages consumed)
                last_flush = time.time()

    finally:
        # Flush remaining on shutdown
        for topic, rows in batches.items():
            if rows:
                flush_batch(s3, topic, rows)
        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            pass  # No offset to commit (e.g. no messages received yet)
        consumer.close()
        logger.info("Consumer shut down. Total written: %d rows", total_written)


if __name__ == "__main__":
    run()
