"""
Rule-based fallback engine — always works, no LLM required.
Handles keyword pattern matching for common ETL analytics queries.
"""
import re
from dataclasses import dataclass


@dataclass
class RulesResult:
    response: str
    generated_sql: str | None
    model_used: str = "rules_engine"
    latency_ms: int = 0


_RULES: list[tuple[re.Pattern, str, str | None]] = [
    (
        re.compile(r"\b(total|sum).*(revenue|sales|amount)\b", re.I),
        "Total revenue analysis: run `SELECT sum(total_amount), toDate(event_ts) FROM etl_warehouse.fact_sales GROUP BY 2 ORDER BY 2 DESC LIMIT 30`",
        "SELECT toDate(event_ts) as date, sum(total_amount) as revenue FROM etl_warehouse.fact_sales WHERE event_ts >= now() - INTERVAL 30 DAY GROUP BY 1 ORDER BY 1",
    ),
    (
        re.compile(r"\b(top|best|highest).*(product|item|sku)\b", re.I),
        "Top products by revenue: aggregating sales by product.",
        "SELECT product_id, sum(total_amount) as revenue, sum(quantity) as units FROM etl_warehouse.fact_sales GROUP BY product_id ORDER BY revenue DESC LIMIT 10",
    ),
    (
        re.compile(r"\b(anomal|spike|outlier|unusual)\b", re.I),
        "Anomaly detection: checking sensor readings for 3σ deviations.",
        "SELECT location_id, hour_ts, avg_temp, anomaly_flag FROM etl_warehouse.agg_hourly_sensor WHERE anomaly_flag = 1 ORDER BY hour_ts DESC LIMIT 50",
    ),
    (
        re.compile(r"\b(trade|volume|asset|crypto|stock)\b", re.I),
        "Trading volume summary: aggregating by asset and date.",
        "SELECT asset, sum(total_volume) as volume, avg(avg_price) as avg_price FROM etl_warehouse.agg_trading_volume WHERE full_date >= today() - 7 GROUP BY asset ORDER BY volume DESC",
    ),
    (
        re.compile(r"\b(social|engagement|like|share|platform)\b", re.I),
        "Social engagement summary: top platforms by weighted engagement.",
        "SELECT platform, sum(likes) as likes, sum(shares) as shares, sum(comments) as comments FROM etl_warehouse.fact_social_engagement GROUP BY platform ORDER BY likes DESC",
    ),
    (
        re.compile(r"\b(sensor|temperature|humidity|pressure|iot)\b", re.I),
        "IoT sensor overview: latest readings per location.",
        "SELECT location_id, avg(temperature) as avg_temp, avg(humidity) as avg_humidity, count(*) as readings FROM etl_warehouse.fact_sensor_readings WHERE event_ts >= now() - INTERVAL 1 HOUR GROUP BY location_id",
    ),
    (
        re.compile(r"\b(pipeline|status|health|lag|kafka)\b", re.I),
        "Pipeline health: check the dashboard topology view at http://localhost:3000 for live Kafka lag and service status.",
        None,
    ),
]

_FALLBACK_RESPONSE = (
    "I couldn't match your query to a known pattern. "
    "Try asking about: revenue, top products, sensor anomalies, trading volume, social engagement, or pipeline health. "
    "For arbitrary queries, use the AI Query mode when Ollama is running."
)


def analyze(query: str) -> RulesResult:
    import time
    t0 = time.monotonic()
    for pattern, response, sql in _RULES:
        if pattern.search(query):
            return RulesResult(
                response=response,
                generated_sql=sql,
                latency_ms=int((time.monotonic() - t0) * 1000),
            )
    return RulesResult(
        response=_FALLBACK_RESPONSE,
        generated_sql=None,
        latency_ms=int((time.monotonic() - t0) * 1000),
    )
