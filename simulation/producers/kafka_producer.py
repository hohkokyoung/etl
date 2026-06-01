"""
Kafka producer base — wraps confluent_kafka for JSON message delivery.
Each generator module calls produce() with its events.
"""
import json
import logging
import os
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient

logger = logging.getLogger(__name__)


def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "acks": "1",
        "compression.type": "lz4",
        "linger.ms": 5,
        "batch.size": 65536,
    })


_producer: Producer | None = None


def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = _make_producer()
    return _producer


def _delivery_report(err, msg):
    if err:
        logger.warning("Delivery failed: %s", err)


def produce(topic: str, event: dict[str, Any], key: str | None = None) -> None:
    p = get_producer()
    p.produce(
        topic=topic,
        key=(key or event.get("event_type", "unknown")).encode(),
        value=json.dumps(event).encode(),
        callback=_delivery_report,
    )
    p.poll(0)  # trigger callbacks without blocking


def flush():
    get_producer().flush(timeout=5)
