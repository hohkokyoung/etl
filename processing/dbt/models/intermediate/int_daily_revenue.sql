-- Intermediate: daily revenue rolled up, joining product category from dim.
-- Used by both the revenue mart and the LLM insight aggregation.

with sales as (
    select * from {{ ref('stg_sales') }}
    where status != 'cancelled'
),

products as (
    select product_id, category
    from {{ source('etl_warehouse', 'dim_product') }}
),

customers as (
    select customer_id, region
    from {{ source('etl_warehouse', 'dim_customer') }}
),

joined as (
    select
        s.date_key,
        toDate(s.event_ts)                  as full_date,
        coalesce(c.region, 'unknown')       as region,
        coalesce(p.category, 'unknown')     as category,
        count(*)                            as total_orders,
        sum(s.total_amount)                 as total_revenue,
        avg(s.total_amount)                 as avg_order_value,
        sum(s.quantity)                     as total_units
    from sales s
    left join products p on s.product_id = p.product_id
    left join customers c on s.customer_id = c.customer_id
    group by 1, 2, 3, 4
)

select * from joined
