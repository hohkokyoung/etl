"""Data lake browser — row counts and size per S3 prefix/layer."""
import os
from typing import Any

import boto3
from fastapi import APIRouter

from cache import get, set as cache_set

router = APIRouter()
CACHE_TTL = int(os.environ.get("API_CACHE_TTL_LAKE", "30"))


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://floci:4566"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name="us-east-1",
    )


def _bucket_stats(s3, bucket: str) -> dict[str, Any]:
    try:
        paginator = s3.get_paginator("list_objects_v2")
        total_size = 0
        total_files = 0
        by_source: dict[str, dict] = {}

        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                size = obj["Size"]
                total_size += size
                total_files += 1
                source = key.split("/")[0].replace("source=", "") if "source=" in key else "other"
                if source not in by_source:
                    by_source[source] = {"files": 0, "size_bytes": 0}
                by_source[source]["files"] += 1
                by_source[source]["size_bytes"] += size

        return {
            "total_files": total_files,
            "total_size_mb": round(total_size / 1024 / 1024, 2),
            "by_source": by_source,
        }
    except Exception as exc:
        return {"error": str(exc)}


@router.get("/stats")
async def lake_stats():
    cached = await get("lake:stats")
    if cached:
        return cached

    s3 = _s3()
    data = {
        "bronze": _bucket_stats(s3, os.environ.get("S3_BUCKET_BRONZE", "raw-bronze")),
        "silver": _bucket_stats(s3, os.environ.get("S3_BUCKET_SILVER", "processed-silver")),
        "gold":   _bucket_stats(s3, os.environ.get("S3_BUCKET_GOLD", "curated-gold")),
    }
    await cache_set("lake:stats", data, ttl=CACHE_TTL)
    return data


@router.get("/buckets")
async def list_buckets():
    s3 = _s3()
    try:
        resp = s3.list_buckets()
        return {"buckets": [b["Name"] for b in resp.get("Buckets", [])]}
    except Exception as exc:
        return {"error": str(exc)}
