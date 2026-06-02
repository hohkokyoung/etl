{{ config(materialized='table') }}

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
