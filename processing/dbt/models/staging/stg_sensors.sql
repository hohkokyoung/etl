with source as (
    select * from {{ source('etl_warehouse', 'fact_sensor_readings') }}
),

cleaned as (
    select
        reading_id,
        location_id,
        date_key,
        temperature,
        pressure,
        humidity,
        lat,
        lon,
        -- flag physical impossibilities
        temperature between -50 and 85   as temp_in_range,
        pressure between 870 and 1085    as pressure_in_range,
        humidity between 0 and 100       as humidity_in_range,
        event_ts,
        _loaded_at
    from source
    where reading_id is not null and location_id is not null
)

select * from cleaned
