-- THE model: prediction snapshots -> one finalized stop event per
-- (city_key, service_date, trip_uid, stop_sequence). v1 finalization method is
-- last_prediction only: NYC VP status/current_stop_sequence are unreliable
-- (dictionary quirk ③), so the last prediction observed before the vehicle
-- plausibly passed the stop stands in for the actual.
--
-- Mechanics:
--   * ignore predictions fetched > prediction_staleness_min after their own
--     predicted event time (stale revisions of a passed stop)
--   * finalize only events at least finalize_horizon_min behind the freshest
--     fetched_at in the data (still-active stops keep accumulating revisions);
--     data-driven watermark keeps the model replayable
--   * schedule context comes via int_trip_matching (all cities, unioned) ->
--     int_gtfs_scheduled_stop_times; the matcher joins on trip_uid because HSL
--     trip_ids are empty. stop_sequence falls back to the static one (NYC quirk ⑤,
--     and HSL omits stop_sequence on nearly all STUs); nearest schedule row wins
--     if a trip serves a stop twice
--   * delay_arr_sec = COALESCE(feed delay, actual - scheduled)  (canonical rule;
--     a city that states arrival.delay flows through the first branch automatically)
--   * direction_id: NYC from the matcher else the ..N/..S trip_id suffix (quirk ⑥ —
--     never the feed's direction_id); every other city from the feed's direction_id
--     else the matched static trip's

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

with preds as (
    select
        p.*,
        m.static_trip_id,
        m.direction_id as matched_direction_id,
        m.match_confidence,
        coalesce(p.arr_pred_ts_utc, p.dep_pred_ts_utc) as event_pred_ts
    from {{ ref('stg_gtfsrt__trip_updates') }} p
    left join {{ ref('int_trip_matching') }} m
      on m.city_key = p.city_key
     and m.service_date = p.service_date
     and m.trip_uid = p.trip_uid
    where coalesce(p.arr_pred_ts_utc, p.dep_pred_ts_utc) is not null
    {% if is_incremental() %}
      and p.service_date >= current_date - cast(ceil({{ var('lookback_hours') }} / 24.0) as int)
    {% endif %}
),

fresh as (
    select * from preds
    where fetched_at <= {{ dbt.dateadd('minute', var('prediction_staleness_min'), 'event_pred_ts') }}
),

watermark as (
    select max(fetched_at) as max_fetched from fresh
),

sched_ranked as (
    select
        f.city_key, f.service_date, f.trip_uid, f.stop_id, f.fetched_at,
        s.stop_sequence as static_stop_sequence,
        s.sched_arr_ts_utc, s.sched_dep_ts_utc, s.timepoint, s.gtfs_version_id,
        row_number() over (
            partition by f.city_key, f.service_date, f.trip_uid, f.stop_id, f.fetched_at
            order by abs({{ seconds_between('f.event_pred_ts', 's.sched_arr_ts_utc') }})
        ) as sched_rn
    from fresh f
    join {{ ref('int_gtfs_scheduled_stop_times') }} s
      on s.city_key = f.city_key
     and s.service_date = f.service_date
     and s.trip_id = f.static_trip_id
     and s.stop_id = f.stop_id
),

enriched as (
    select
        f.*,
        sr.static_stop_sequence,
        sr.sched_arr_ts_utc,
        sr.sched_dep_ts_utc,
        sr.timepoint,
        sr.gtfs_version_id,
        coalesce(f.stop_sequence, sr.static_stop_sequence) as stop_sequence_eff
    from fresh f
    left join sched_ranked sr
      on sr.city_key = f.city_key and sr.service_date = f.service_date
     and sr.trip_uid = f.trip_uid and sr.stop_id = f.stop_id
     and sr.fetched_at = f.fetched_at and sr.sched_rn = 1
),

ranked as (
    select
        e.*,
        row_number() over (
            partition by e.city_key, e.service_date, e.trip_uid, e.stop_sequence_eff
            order by e.fetched_at desc, e.event_pred_ts desc
        ) as rn,
        count(*) over (
            partition by e.city_key, e.service_date, e.trip_uid, e.stop_sequence_eff
        ) as prediction_count,
        min(e.fetched_at) over (
            partition by e.city_key, e.service_date, e.trip_uid, e.stop_sequence_eff
        ) as first_seen_utc,
        max(e.fetched_at) over (
            partition by e.city_key, e.service_date, e.trip_uid, e.stop_sequence_eff
        ) as last_seen_utc
    from enriched e
    cross join watermark w
    -- A stop visit needs a stop. HSL emits a few updates carrying stop_sequence
    -- but no stop_id (892 of 7.2M silver rows, 849 of them with a sequence, so
    -- they clear the sequence filter above): they cannot be placed at a stop,
    -- joined to the schedule, or paired into a headway, and they arrived in
    -- fct_headways as 225 null stop_ids breaking its not-null contract. Same
    -- class of rule as the sequence filter — no identity, no event.
    where e.stop_sequence_eff is not null
      and e.stop_id is not null
      and e.event_pred_ts <= w.max_fetched - interval '{{ var("finalize_horizon_min") }} minutes'
)

select
    city_key,
    service_date,
    trip_uid,
    stop_sequence_eff as stop_sequence,
    trip_id as trip_id_raw,
    static_trip_id,
    route_id,
    case
        when city_key = 'nyc' then coalesce(
            matched_direction_id,
            case {{ re_extract('trip_id', '\.\.?([NS])', 1) }} when 'N' then 0 when 'S' then 1 end
        )
        else coalesce(direction_id, matched_direction_id)
    end as direction_id,
    stop_id,
    vehicle_id,
    match_confidence,
    gtfs_version_id,
    timepoint,
    sched_arr_ts_utc,
    sched_dep_ts_utc,
    arr_pred_ts_utc as actual_arr_ts_utc,
    dep_pred_ts_utc as actual_dep_ts_utc,
    cast(coalesce(
        arr_delay_sec,
        {{ seconds_between('sched_arr_ts_utc', 'coalesce(arr_pred_ts_utc, dep_pred_ts_utc)') }}
    ) as integer) as delay_arr_sec,
    cast(coalesce(
        dep_delay_sec,
        {{ seconds_between('sched_dep_ts_utc', 'dep_pred_ts_utc') }}
    ) as integer) as delay_dep_sec,
    schedule_relationship,
    (schedule_relationship = 'CANCELED') as cancelled_flag,
    (stu_schedule_relationship = 'SKIPPED') as skipped_flag,
    'last_prediction' as finalization_method,
    prediction_count,
    first_seen_utc,
    last_seen_utc,
    source_format
from ranked
where rn = 1
