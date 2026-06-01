with source as (
    select * from {{ source('etl_warehouse', 'fact_social_engagement') }}
),

cleaned as (
    select
        post_id,
        user_id,
        lower(trim(platform))      as platform,
        lower(trim(content_type))  as content_type,
        likes,
        shares,
        comments,
        likes + shares * 3 + comments * 2   as weighted_engagement,
        date_key,
        event_ts,
        _loaded_at
    from source
    where post_id is not null and platform is not null
)

select * from cleaned
