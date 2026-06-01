with source as (
    select * from {{ source('etl_warehouse', 'fact_trades') }}
),

cleaned as (
    select
        trade_id,
        upper(trim(asset))   as asset,
        lower(trim(side))    as side,
        quantity,
        price,
        total_value,
        date_key,
        event_ts,
        _loaded_at
    from source
    where
        trade_id is not null
        and quantity > 0
        and price > 0
        and side in ('buy', 'sell')
)

select * from cleaned
