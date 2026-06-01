{{
    config(
        materialized='incremental',
        engine='SummingMergeTree()',
        order_by='(hour_ts, location_id)',
        partition_by='toYYYYMMDD(hour_ts)',
        incremental_strategy='append',
    )
}}

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

{% if is_incremental() %}
where hour_ts >= (select max(hour_ts) from {{ this }}) - interval 2 hour
{% endif %}
