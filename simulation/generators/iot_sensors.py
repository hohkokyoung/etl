import uuid
import random
import math
from datetime import datetime, timezone

SENSOR_LOCATIONS = [
    {"location_id": "LOC-001", "site_name": "Warehouse A", "lat": 3.1390, "lon": 101.6869},
    {"location_id": "LOC-002", "site_name": "Factory B",   "lat": 1.3521, "lon": 103.8198},
    {"location_id": "LOC-003", "site_name": "Office C",    "lat": 13.7563, "lon": 100.5018},
    {"location_id": "LOC-004", "site_name": "Depot D",     "lat": 22.3193, "lon": 114.1694},
    {"location_id": "LOC-005", "site_name": "Plant E",     "lat": 37.5665, "lon": 126.9780},
]

SENSOR_TYPES = ["temperature", "pressure", "humidity", "vibration", "air_quality"]

# Simulate slow drift with occasional anomalies
_base_temps = {loc["location_id"]: random.uniform(20.0, 30.0) for loc in SENSOR_LOCATIONS}


def _drift(base: float, std: float = 0.5) -> float:
    return round(base + random.gauss(0, std), 2)


def generate_reading() -> dict:
    loc = random.choice(SENSOR_LOCATIONS)
    lid = loc["location_id"]

    # Occasionally inject an anomaly (5% chance)
    is_anomaly = random.random() < 0.05
    temp_base = _base_temps[lid]
    if is_anomaly:
        temp = round(temp_base + random.uniform(10, 25), 2)
    else:
        temp = _drift(temp_base)
        _base_temps[lid] = temp  # slow drift

    pressure = round(random.gauss(1013.25, 5), 2)
    humidity = round(min(100, max(0, random.gauss(60, 10))), 2)
    vibration = round(random.exponential(0.1) if is_anomaly else random.exponential(0.02), 4)

    return {
        "reading_id": str(uuid.uuid4()),
        "location_id": lid,
        "site_name": loc["site_name"],
        "sensor_type": random.choice(SENSOR_TYPES),
        "temperature": temp,
        "pressure": pressure,
        "humidity": humidity,
        "vibration": vibration,
        "lat": loc["lat"] + random.gauss(0, 0.0001),
        "lon": loc["lon"] + random.gauss(0, 0.0001),
        "is_anomaly": is_anomaly,
        "battery_pct": round(random.uniform(20, 100), 1),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "event_type": "sensor_reading",
        "source": "iot",
    }


def generate_event() -> dict:
    return generate_reading()
