-- Observed consecutive arrivals per (city, route, direction, stop, service_date),
-- one row per arrival that has a predecessor. ADDED trips are INCLUDED here —
-- headways measure the service actually delivered, unlike OTP which excludes
-- them (locked rule). CANCELED/SKIPPED events never carry an arrival.
--
-- sched_headway_sec: NYC publishes no frequencies.txt (docs/02 fct_headways),
-- so it derives from consecutive *scheduled* arrivals at the same
-- (route, direction, stop): each observed arrival takes the gap owned by the
-- nearest preceding scheduled arrival.

-- [rev 2026-08-25] on_schema_change matches fct_stop_events. dbt's default is
-- 'ignore', which silently computes a new column in the SELECT and never adds it to
-- the existing relation — exactly how stale_observation_flag once failed its own
-- test. Three sibling incrementals carried the guard; this one did not.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    unique_key=['city_key', 'service_date', 'trip_uid', 'stop_sequence']
) }}

with events as (

    select
        city_key,
        service_date,
        route_id,
        route_key,  -- SCD2 FKs resolved once, in fct_stop_events; inherited here
        coalesce(direction_id, -1) as direction_id,  -- match the -1 fold used in facts
        stop_id,
        stop_key,
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
    from {{ ref('int_scheduled_stop_times') }}
    where sched_arr_ts_utc is not null
    {% if is_incremental() %}
      and service_date >= current_date - cast(ceil({{ var('lookback_hours') }} / 24.0) as int)
    {% endif %}

),

paired as (

    -- Same gate as fct_stop_events: where realtime and static disagree on stop
    -- ids, a matched scheduled gap is a collision between unrelated stops, so
    -- sched_headway_sec is nulled — and gap_ratio, bunched_flag and big_gap_flag
    -- all divide by it, so they null out with it. actual_gap_sec is pure
    -- observation and survives, which is why these cities still get real
    -- headways and wait-time regularity.
    select
        o.*,
        case when coalesce(c.schedule_matchable, true) then s.sched_gap_sec end
            as sched_headway_sec
    from obs o
    left join {{ ref('dim_city') }} c on c.city_key = o.city_key
    left join sched_gaps s
      on s.city_key = o.city_key
     and s.service_date = o.service_date
     and s.route_id = o.route_id
     and s.direction_id = o.direction_id
     and s.stop_id = o.stop_id
     and s.sched_arr_ts_utc <= o.actual_arr_ts_utc
    where o.prev_arr_ts_utc is not null
      -- [rev 2026-09-11] A gap that overlaps a stretch the city's feed was dark
      -- (int_feed_gaps) measures nothing: either an endpoint is a frozen pre-outage
      -- prediction scored as an arrival, or the two real arrivals straddle hours
      -- nobody saw. Dropping the gap — not the arrival — keeps the lag sequence honest
      -- on both sides of the outage instead of manufacturing one six-hour headway.
      and not exists (
          select 1 from {{ ref('int_feed_gaps') }} g
          where g.city_key = o.city_key
            and o.prev_arr_ts_utc < g.gap_end_utc
            and o.actual_arr_ts_utc > g.gap_start_utc
      )
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
    route_key,
    direction_id,
    stop_id,
    stop_key,
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
