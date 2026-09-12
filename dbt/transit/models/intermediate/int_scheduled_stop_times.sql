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
