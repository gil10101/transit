-- Typed interface over silver.stop_time_predictions. Canonical delay coalesce is
-- deferred to int_stop_events_finalized (needs the schedule join).

select
    city as city_key,
    agency,
    endpoint,
    source_format,
    feed_ts,
    fetched_at,
    service_date,
    trip_uid,
    trip_id,
    route_id,
    direction_id,
    start_date,
    start_time,
    schedule_relationship,
    vehicle_id,
    stop_id,
    stop_sequence,
    arr_pred_ts_utc,
    arr_delay_sec,
    arr_uncertainty,
    dep_pred_ts_utc,
    dep_delay_sec,
    stu_schedule_relationship
from {{ source('silver', 'stop_time_predictions') }}
