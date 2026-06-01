"""Simulation control router — read/write the rate stored in Valkey."""
import redis
import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()

VALKEY_SYNC_URL = os.environ.get("VALKEY_URL", "redis://valkey:6379/0").replace("/1", "/0")
_r = redis.from_url(VALKEY_SYNC_URL, decode_responses=True)


class RateUpdate(BaseModel):
    events_per_second: int = Field(ge=0, le=10000)


@router.get("/status")
def simulation_status():
    rate = _r.get("sim:events_per_second") or "0"
    enabled = _r.get("sim:enabled") or "true"
    return {
        "events_per_second": int(rate),
        "enabled": enabled.lower() == "true",
    }


@router.post("/rate")
def set_rate(body: RateUpdate):
    _r.set("sim:events_per_second", body.events_per_second)
    return {"events_per_second": body.events_per_second, "status": "updated"}


@router.post("/start")
def start_simulation():
    _r.set("sim:enabled", "true")
    _r.set("sim:events_per_second", _r.get("sim:events_per_second") or "50")
    return {"status": "started"}


@router.post("/stop")
def stop_simulation():
    _r.set("sim:events_per_second", "0")
    return {"status": "stopped"}


@router.get("/counters")
def simulation_counters():
    """Returns per-topic event counters published by the scheduler."""
    keys = _r.keys("sim:counter:*")
    return {k.replace("sim:counter:", ""): int(_r.get(k) or 0) for k in keys}
