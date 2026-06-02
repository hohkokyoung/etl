{{ config(materialized='table') }}

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
