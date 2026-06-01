{{
    config(
        materialized='incremental',
        engine='SummingMergeTree()',
        order_by='(full_date, region, category)',
        partition_by='toYYYYMM(full_date)',
        incremental_strategy='append',
    )
}}

select
    date_key,
    full_date,
    region,
    category,
    total_orders,
    total_revenue,
    avg_order_value,
    total_units,
    now()   as _computed_at
from {{ ref('int_daily_revenue') }}

{% if is_incremental() %}
where full_date >= (select max(full_date) from {{ this }}) - 1
{% endif %}
