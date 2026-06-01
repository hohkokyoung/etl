"""Valkey (Redis-compatible) cache helpers for the dashboard API."""
import json
import os
from typing import Any

import redis.asyncio as aioredis

VALKEY_URL = os.environ.get("VALKEY_URL", "redis://localhost:6379/1")

_pool: aioredis.Redis | None = None


def get_client() -> aioredis.Redis:
    global _pool
    if _pool is None:
        _pool = aioredis.from_url(VALKEY_URL, decode_responses=True)
    return _pool


async def get(key: str) -> Any | None:
    r = get_client()
    raw = await r.get(key)
    return json.loads(raw) if raw else None


async def set(key: str, value: Any, ttl: int = 30) -> None:
    r = get_client()
    await r.setex(key, ttl, json.dumps(value, default=str))


async def publish(channel: str, message: Any) -> None:
    r = get_client()
    await r.publish(channel, json.dumps(message, default=str))
