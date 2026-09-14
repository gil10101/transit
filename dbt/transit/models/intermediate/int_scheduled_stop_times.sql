-- Every city's scheduled stop times in one relation: the GTFS path for the
-- seven GTFS-RT cities, the odpt:TrainTimetable path for Tokyo.
--
-- fct_headways and int_service_frequency read THIS rather than
-- int_gtfs_scheduled_stop_times, because they are the two models that compare
-- an observed sequence against a scheduled one and must therefore see every
-- city that produces observed arrivals. Tokyo produced 362,473 observed gaps
-- against zero scheduled ones until this existed (see
-- int_odpt_scheduled_stop_times for what that silently nulled).
--
-- This is additive, not a swap. The GTFS model keeps its other five consumers
-- -- int_service_dates, int_service_dates_pit, int_gtfs_version_for_date,
-- fct_service_delivery_daily and int_stop_events_finalized -- because each is
-- GTFS-specific by construction: they resolve calendars, static versions and
-- trip matching, none of which Tokyo's schedule participates in. Tokyo reaches
-- those through int_odpt_scheduled_trips and int_odpt_stop_events instead.
-- Pointing them here would hand them a trip_id that is a TrainTimetable URN and
-- a gtfs_version_id that describes stops rather than trips.

-- var('pit_cities') routes named cities through the point-in-time schedule
-- instead of the newest-pinned one, so history deleted by a static rotation can
-- be recomputed against the static that was actually live. The two sources are
-- disjoint by city, never unioned for the same (city, date): two identical
-- scheduled arrivals would make a zero-second gap and null every ratio built on
-- it. Empty by default, which leaves the steady-state chain untouched.
{% set pit_cities = var('pit_cities', '') %}
{% set pit_list = pit_cities.split(',') | map('trim') | reject('equalto', '') | list %}

select
    city_key,
    service_date,
    trip_id,
    route_id,
    direction_id,
    stop_id,
    stop_sequence,
    timepoint,
    gtfs_version_id,
    sched_arr_ts_utc,
    sched_dep_ts_utc
from {{ ref('int_gtfs_scheduled_stop_times') }}
{% if pit_list %}
where city_key not in ({{ "'" ~ pit_list | join("','") ~ "'" }})
{% endif %}

{% if pit_list %}
union all

select
    city_key,
    service_date,
    trip_id,
    route_id,
    direction_id,
    stop_id,
    stop_sequence,
    timepoint,
    gtfs_version_id,
    sched_arr_ts_utc,
    sched_dep_ts_utc
from {{ ref('int_gtfs_scheduled_stop_times_pit') }}
{% endif %}

union all

select
    city_key,
    service_date,
    trip_id,
    route_id,
    direction_id,
    stop_id,
    stop_sequence,
    timepoint,
    gtfs_version_id,
    sched_arr_ts_utc,
    sched_dep_ts_utc
from {{ ref('int_odpt_scheduled_stop_times') }}
