{{ config(materialized='table') }}

select
    location_id,
    hour_ts,
    avg_temp,
    avg_pressure,
    avg_humidity,
    reading_count,
    temp_anomaly     as anomaly_flag,
    now()            as _computed_at
from {{ ref('int_sensor_anomalies') }}
