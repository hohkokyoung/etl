"""
Text-to-SQL: converts a natural language question into ClickHouse SQL
by injecting the warehouse schema as context into the LLM prompt.
"""
import re

# ClickHouse schema context injected into every text-to-SQL prompt
SCHEMA_CONTEXT = """
You are a ClickHouse SQL expert. The database is `etl_warehouse`. Available tables:

fact_sales(sale_id String, order_id String, customer_id String, product_id String,
           date_key UInt32, quantity UInt32, unit_price Decimal(10,2),
           total_amount Decimal(12,2), status String, event_ts DateTime)

fact_sensor_readings(reading_id String, location_id String, date_key UInt32,
                     temperature Float32, pressure Float32, humidity Float32,
                     lat Float64, lon Float64, event_ts DateTime)

fact_trades(trade_id String, asset String, side String, quantity Float64,
            price Decimal(18,8), total_value Decimal(18,4), date_key UInt32, event_ts DateTime)

fact_social_engagement(post_id String, user_id String, platform String,
                        content_type String, likes UInt32, shares UInt32,
                        comments UInt32, date_key UInt32, event_ts DateTime)

dim_customer(customer_id String, email String, name String, region String, created_at DateTime)
dim_product(product_id String, name String, category String, price Decimal(10,2))
dim_sensor_location(location_id String, site_name String, latitude Float64, longitude Float64)
dim_date(date_key UInt32, full_date Date, year UInt16, quarter UInt8, month UInt8, day_of_week UInt8)

agg_daily_revenue(date_key UInt32, full_date Date, region String, category String,
                  total_orders UInt32, total_revenue Decimal(18,2), avg_order_value Decimal(10,2))
agg_hourly_sensor(hour_ts DateTime, location_id String, avg_temp Float32,
                  avg_pressure Float32, avg_humidity Float32, reading_count UInt32, anomaly_flag UInt8)
agg_trading_volume(date_key UInt32, full_date Date, asset String, total_volume Float64,
                   trade_count UInt32, avg_price Decimal(18,8))

llm_insights(insight_id String, query_type String, user_query String,
             generated_sql Nullable(String), response String, model_used String,
             latency_ms UInt32, created_at DateTime)

Rules:
- Always prefix table names with `etl_warehouse.`
- Use toDate(), toStartOfHour(), toYYYYMM() for date functions
- Prefer LIMIT to avoid large scans
- Return ONLY the SQL query, no explanation, no markdown fences
""".strip()


def build_prompt(user_question: str) -> str:
    return f"{SCHEMA_CONTEXT}\n\nQuestion: {user_question}\n\nSQL:"


def extract_sql(raw: str) -> str:
    """Strip markdown fences and whitespace from LLM output."""
    raw = raw.strip()
    # Remove ```sql ... ``` or ``` ... ```
    raw = re.sub(r"^```(?:sql)?\s*", "", raw, flags=re.I)
    raw = re.sub(r"\s*```$", "", raw)
    # Take only the first statement
    parts = raw.split(";")
    sql = parts[0].strip() + ";"
    return sql


def is_safe_sql(sql: str) -> bool:
    """Reject mutations — only allow SELECT."""
    upper = sql.upper().strip()
    if not upper.startswith("SELECT"):
        return False
    dangerous = ["DROP", "TRUNCATE", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "SYSTEM"]
    return not any(kw in upper for kw in dangerous)
