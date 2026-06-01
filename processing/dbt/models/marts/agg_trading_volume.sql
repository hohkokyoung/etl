{{
    config(
        materialized='incremental',
        engine='SummingMergeTree()',
        order_by='(full_date, asset)',
        partition_by='toYYYYMM(full_date)',
        incremental_strategy='append',
    )
}}

with trades as (
    select * from {{ ref('stg_trades') }}
)

select
    date_key,
    toDate(event_ts)       as full_date,
    asset,
    sum(quantity)          as total_volume,
    count(*)               as trade_count,
    avg(price)             as avg_price,
    sum(total_value)       as total_value,
    countIf(side = 'buy')  as buy_count,
    countIf(side = 'sell') as sell_count,
    now()                  as _computed_at
from trades
group by 1, 2, 3

{% if is_incremental() %}
having full_date >= (select max(full_date) from {{ this }}) - 1
{% endif %}
