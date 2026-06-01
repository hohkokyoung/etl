"""
Simulation scheduler — reads target rate from Valkey and drives all generators.
Runs indefinitely; safe to SIGTERM (graceful shutdown).
"""
import logging
import os
import signal
import time
from typing import Callable

import redis

from generators import ecommerce, iot_sensors, financial, social
from producers.kafka_producer import produce, flush

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

VALKEY_URL = os.environ.get("VALKEY_URL", "redis://localhost:6379/0")
DEFAULT_RATE = int(os.environ.get("SIM_EVENTS_PER_SECOND", "50"))

TOPIC_MAP: list[tuple[str, Callable]] = [
    ("transactions", ecommerce.generate_event),
    ("sensors", iot_sensors.generate_event),
    ("financial", financial.generate_event),
    ("social", social.generate_event),
]

_running = True


def _handle_sigterm(sig, frame):
    global _running
    logger.info("SIGTERM received — stopping simulation.")
    _running = False


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def get_target_rate(r: redis.Redis) -> int:
    val = r.get("sim:events_per_second")
    return int(val) if val else DEFAULT_RATE


def run():
    r = redis.from_url(VALKEY_URL, decode_responses=True)
    r.set("sim:events_per_second", DEFAULT_RATE, nx=True)  # set default if not already set

    logger.info("Simulation scheduler started. Initial rate: %d events/sec", DEFAULT_RATE)

    counters = {topic: 0 for topic, _ in TOPIC_MAP}
    last_log = time.time()

    while _running:
        rate = get_target_rate(r)
        if rate == 0:
            time.sleep(1)
            continue

        interval = 1.0 / max(1, rate)
        cycle_start = time.time()

        # Round-robin across all generators each cycle
        topic, generator = TOPIC_MAP[int(time.time() * rate) % len(TOPIC_MAP)]
        try:
            event = generator()
            produce(topic, event)
            counters[topic] += 1
        except Exception as exc:
            logger.warning("Generator error (%s): %s", topic, exc)

        # Log throughput every 10 seconds
        if time.time() - last_log >= 10:
            total = sum(counters.values())
            logger.info(
                "Throughput | rate=%d/s | total=%d | %s",
                rate,
                total,
                " | ".join(f"{t}={c}" for t, c in counters.items()),
            )
            last_log = time.time()

        elapsed = time.time() - cycle_start
        sleep_time = max(0, interval - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)

    flush()
    logger.info("Simulation stopped. Total events produced: %d", sum(counters.values()))


if __name__ == "__main__":
    run()
