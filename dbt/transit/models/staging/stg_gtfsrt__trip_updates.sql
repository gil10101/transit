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
--
-- DC branch: WMATA rail marks its extra 'NR' shuttle runs UNSCHEDULED (fixture
-- 2026-08-23: 2/142 rail TUs, trip_ids NR268/NR023 — absent from static; bus
-- never emits it). Same locked-rule semantics as ADDED (volume yes, OTP no),
-- so UNSCHEDULED -> ADDED keeps them on the generic ADDED path (trips_added,
-- excluded from OTP and from the completeness numerator). Staging-only, same
-- as toronto. Guarded by tests/assert_dc_unscheduled_normalized.sql.
--
-- stu_schedule_relationship passes through RAW: SKIPPED stops (WMATA bus marks
-- ~30% of STUs SKIPPED, fixture 2026-08-23) become skipped_flag downstream and
-- are excluded from OTP/headways there — dropping them here would lose the
-- skip signal itself.

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
        when city = 'dc' and schedule_relationship = 'UNSCHEDULED'
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
