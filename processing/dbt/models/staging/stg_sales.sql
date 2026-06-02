-- Staging: pull fact_sales from ClickHouse warehouse (loaded by Airflow/Spark)
-- Applies light cleaning and naming standardisation.

with source as (
    select * from {{ source('etl_warehouse', 'fact_sales') }}
),

cleaned as (
    select
        sale_id,
        order_id,
        customer_id,
        product_id,
        date_key,
        toUInt32(quantity)                          as quantity,
        toDecimal64(unit_price, 2)                  as unit_price,
        toDecimal64(total_amount, 2)                as total_amount,
        lower(trim(status))                         as status,
        event_ts,
        _batch_id,
        _loaded_at
    from source
    where
        sale_id is not null
        and customer_id is not null
        and product_id is not null
        and total_amount > 0
)

select * from cleaned
