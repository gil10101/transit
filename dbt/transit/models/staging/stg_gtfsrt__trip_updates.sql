-- Typed interface over silver.stop_time_predictions. Canonical delay coalesce is
-- deferred to int_stop_events_finalized (needs the schedule join).
--
-- Toronto branch: TTC's ~3% extra runs carry synthetic NEGATIVE trip_ids and are
-- marked schedule_relationship NEW (the 2024+ GTFS-RT successor of ADDED;
-- fixture-verified 2026-08-23 — 44/1522 trips, all NEW). Downstream implements
-- the locked ADDED rule (service volume yes, OTP no), so normalize here:
-- NEW -> ADDED, and negative-id trips left unset by the feed (the adapter emits
-- the proto default SCHEDULED for unset) -> ADDED as well. Staging-only by
-- design — silver stores what the feed said (dictionary §B TTC row).

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
    case
        when city = 'toronto'
         and (
             schedule_relationship = 'NEW'
             or (trip_id like '-%' and coalesce(schedule_relationship, 'SCHEDULED') = 'SCHEDULED')
         )
        then 'ADDED'
        else schedule_relationship
    end as schedule_relationship,
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
