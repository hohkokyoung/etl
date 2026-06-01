-- Intermediate: hourly sensor aggregation with anomaly flagging.
-- Anomaly = reading outside 3σ of the location's 24-hour window.

with sensors as (
    select * from {{ ref('stg_sensors') }}
    where temp_in_range and pressure_in_range and humidity_in_range
),

stats as (
    select
        location_id,
        toStartOfDay(event_ts)               as day_ts,
        avg(temperature)                     as avg_temp_day,
        stddevPop(temperature)               as std_temp_day,
        avg(pressure)                        as avg_pressure_day,
        stddevPop(pressure)                  as std_pressure_day
    from sensors
    group by 1, 2
),

hourly as (
    select
        s.location_id,
        toStartOfHour(s.event_ts)            as hour_ts,
        avg(s.temperature)                   as avg_temp,
        avg(s.pressure)                      as avg_pressure,
        avg(s.humidity)                      as avg_humidity,
        count(*)                             as reading_count
    from sensors s
    group by 1, 2
),

annotated as (
    select
        h.*,
        st.avg_temp_day,
        st.std_temp_day,
        abs(h.avg_temp - st.avg_temp_day) > 3 * st.std_temp_day as temp_anomaly
    from hourly h
    left join stats st
        on h.location_id = st.location_id
        and toStartOfDay(h.hour_ts) = st.day_ts
)

select * from annotated
