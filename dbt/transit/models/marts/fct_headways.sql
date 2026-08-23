-- P5: natural keys until SCD2 dims (P6)
--
-- Observed consecutive arrivals per (city, route, direction, stop, service_date),
-- one row per arrival that has a predecessor. ADDED trips are INCLUDED here —
-- headways measure the service actually delivered, unlike OTP which excludes
-- them (locked rule). CANCELED/SKIPPED events never carry an arrival.
--
-- sched_headway_sec: NYC publishes no frequencies.txt (docs/02 fct_headways),
-- so it derives from consecutive *scheduled* arrivals at the same
-- (route, direction, stop): each observed arrival takes the gap owned by the
-- nearest preceding scheduled arrival.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['city_key', 'service_date', 'trip_uid', 'stop_sequence']
) }}

with events as (

    select
        city_key,
        service_date,
        route_id,
        coalesce(direction_id, -1) as direction_id,  -- match the -1 fold used in facts
        stop_id,
        stop_sequence,
        trip_uid,
        vehicle_id,
        actual_arr_ts_utc,
        local_hour
    from {{ ref('fct_stop_events') }}
    where actual_arr_ts_utc is not null
      and coalesce(schedule_relationship, 'SCHEDULED') in ('SCHEDULED', 'ADDED')
      and not coalesce(skipped_flag, false)
    {% if is_incremental() %}
      and service_date >= current_date - cast(ceil({{ var('lookback_hours') }} / 24.0) as int)
    {% endif %}

),

obs as (

    -- window inlined 3x: Snowflake has no named-WINDOW clause
    select
        *,
        lag(trip_uid) over (
            partition by city_key, route_id, direction_id, stop_id, service_date
            order by actual_arr_ts_utc, trip_uid
        ) as prev_trip_uid,
        lag(vehicle_id) over (
            partition by city_key, route_id, direction_id, stop_id, service_date
            order by actual_arr_ts_utc, trip_uid
        ) as prev_vehicle_id,
        lag(actual_arr_ts_utc) over (
            partition by city_key, route_id, direction_id, stop_id, service_date
            order by actual_arr_ts_utc, trip_uid
        ) as prev_arr_ts_utc
    from events

),

sched_gaps as (

    select
        city_key,
        service_date,
        route_id,
        direction_id,
        stop_id,
        sched_arr_ts_utc,
        {{ seconds_between(
            "lag(sched_arr_ts_utc) over (
                partition by city_key, route_id, direction_id, stop_id, service_date
                order by sched_arr_ts_utc)",
            "sched_arr_ts_utc") }} as sched_gap_sec
    from {{ ref('int_gtfs_scheduled_stop_times') }}
    where sched_arr_ts_utc is not null
    {% if is_incremental() %}
      and service_date >= current_date - cast(ceil({{ var('lookback_hours') }} / 24.0) as int)
    {% endif %}

),

paired as (

    select
        o.*,
        s.sched_gap_sec as sched_headway_sec
    from obs o
    left join sched_gaps s
      on s.city_key = o.city_key
     and s.service_date = o.service_date
     and s.route_id = o.route_id
     and s.direction_id = o.direction_id
     and s.stop_id = o.stop_id
     and s.sched_arr_ts_utc <= o.actual_arr_ts_utc
    where o.prev_arr_ts_utc is not null
    qualify row_number() over (
        partition by o.city_key, o.service_date, o.trip_uid, o.stop_sequence
        order by s.sched_arr_ts_utc desc
    ) = 1

),

final as (

    select
        *,
        {{ seconds_between('prev_arr_ts_utc', 'actual_arr_ts_utc') }} as actual_gap_sec
    from paired

)

select
    md5(concat_ws('|', city_key, service_date, trip_uid, stop_sequence)) as headway_key,
    city_key,
    service_date,
    route_id,
    direction_id,
    stop_id,
    stop_sequence,
    trip_uid,
    prev_trip_uid,
    vehicle_id,
    prev_vehicle_id,
    local_hour,
    {{ daypart('local_hour') }} as daypart,
    actual_arr_ts_utc,
    prev_arr_ts_utc,
    cast(actual_gap_sec as integer) as actual_gap_sec,
    cast(sched_headway_sec as integer) as sched_headway_sec,
    cast(actual_gap_sec as double) / nullif(sched_headway_sec, 0) as gap_ratio,
    (cast(actual_gap_sec as double) / nullif(sched_headway_sec, 0)) < 0.5 as bunched_flag,
    (cast(actual_gap_sec as double) / nullif(sched_headway_sec, 0)) > 2.0 as big_gap_flag
from final
