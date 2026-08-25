-- Atomic fact: one finalized stop visit per (city_key, service_date, trip_uid,
-- stop_sequence). Local-time fields derive via dim_city.iana_tz; all analysis is
-- local, storage is UTC.
--
-- early_departure_flag (locked rule): bus departure > early_departure_grace_sec
-- early at a scheduled timepoint. Live where the static carries timepoint
-- (MBTA, HSL); false where it omits the column (NYC rail; TTC bus-defaults-none
-- per dictionary §D). Requires the P3 canonical static projection + a reparse.

-- on_schema_change: a new column must reach the existing table rather than be
-- silently dropped. The default ignores schema drift, so an added column lives
-- in the model and not in the relation until someone thinks to full-refresh —
-- which is exactly how stale_observation_flag first failed its own test.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['city_key', 'service_date', 'trip_uid', 'stop_sequence'],
    on_schema_change='append_new_columns'
) }}

with f as (
    select * from {{ ref('int_stop_events_finalized') }}
    {% if is_incremental() %}
    where service_date >= current_date - cast(ceil({{ var('lookback_hours') }} / 24.0) as int)
    {% endif %}
),

localized as (
    select
        f.*,
        c.iana_tz,
        c.schedule_matchable,
        c.peak_am_start, c.peak_am_end, c.peak_pm_start, c.peak_pm_end,
        r.mode,
        {{ to_local('f.actual_arr_ts_utc', 'c.iana_tz') }} as actual_arr_ts_local
    from f
    join {{ ref('dim_city') }} c on c.city_key = f.city_key
    left join {{ ref('stg_gtfs__routes') }} r
      on r.city_key = f.city_key and r.route_id = f.route_id
)

select
    md5(concat_ws('|', city_key, service_date, trip_uid, stop_sequence)) as event_key,
    city_key,
    service_date,
    trip_uid,
    stop_sequence,
    trip_id_raw,
    static_trip_id,
    route_id,
    direction_id,
    stop_id,
    vehicle_id,
    match_confidence,
    cast(actual_arr_ts_local as date) as local_date,
    extract(hour from actual_arr_ts_local) as local_hour,
    dayofweek(cast(actual_arr_ts_local as date)) as local_dow,
    (
        cast(actual_arr_ts_local as time) between peak_am_start and peak_am_end
        or cast(actual_arr_ts_local as time) between peak_pm_start and peak_pm_end
    ) and dayofweek(cast(actual_arr_ts_local as date)) between 1 and 5 as is_peak,
    -- A city whose realtime and static feeds do not share a stop namespace has
    -- no trustworthy schedule context at all, so EVERY schedule-derived column
    -- is nulled, not just otp_band. Toronto's 0.32% of events that did join
    -- (3,878 of 1,202,069) are numeric collisions between unrelated stops, and
    -- until 2026-08-25 their fictional delays reached the headline mart as
    -- mean/median/p90 delay on 150 route-days. Actual arrival and departure
    -- times are observations, not schedule context — they stay.
    case when coalesce(schedule_matchable, true) then sched_arr_ts_utc end as sched_arr_ts_utc,
    case when coalesce(schedule_matchable, true) then sched_dep_ts_utc end as sched_dep_ts_utc,
    actual_arr_ts_utc,
    actual_dep_ts_utc,
    case when coalesce(schedule_matchable, true) then delay_arr_sec end as delay_arr_sec,
    case when coalesce(schedule_matchable, true) then delay_dep_sec end as delay_dep_sec,
    -- locked rule: early BUS departure at a scheduled timepoint (> grace) is a
    -- service failure. timepoint is null where the static omits the column
    -- (NYC: rail anyway; TTC: bus-defaults-none per §D -> flag stays false there)
    coalesce(
        mode = 'bus' and timepoint = 1
        and delay_dep_sec < -{{ var('early_departure_grace_sec') }},
        false
    ) as early_departure_flag,
    -- An event finalized from a prediction we last saw long before the event is
    -- a schedule echo, not an observation: the feed stopped covering the trip
    -- (or our polling gapped) and the stale estimate became the "actual"
    -- arrival. Measured 2026-08-24: NYC median lead is 0 min and p95 is 45, but
    -- 3,840 events (3.6%) exceeded 60 min, up to 205. HSL is the extreme case —
    -- it publishes trips up to 3 days ahead, so a poll can carry an arrival
    -- "prediction" for a journey that has not begun.
    coalesce(
        {{ seconds_between('last_seen_utc', 'actual_arr_ts_utc') }}
            > {{ var('max_prediction_lead_min') }} * 60,
        false
    ) as stale_observation_flag,
    -- Neither a SKIPPED stop nor a stale observation is an on-time observation:
    -- the vehicle never served the stop (WMATA bus marks ~30% of STUs SKIPPED,
    -- fixtures 2026-08-23), or we never saw it do so. Null band drops both from
    -- every count(otp_band) numerator AND denominator. The rows stay, so
    -- service volume and completeness still count the trip — the same split the
    -- locked rule makes for ADDED trips.
    case
        when coalesce(skipped_flag, false) then null
        when coalesce(
            {{ seconds_between('last_seen_utc', 'actual_arr_ts_utc') }}
                > {{ var('max_prediction_lead_min') }} * 60,
            false
        ) then null
        -- A city whose realtime stop ids do not share a namespace with its own
        -- static feed cannot be scored against schedule at all. Toronto's
        -- bustime feed and CKAN GTFS agree on 0-6 of ~200 stops per route
        -- (verified 2026-08-24), so the handful that do join are numeric
        -- collisions between unrelated stops and their delays are fiction.
        -- Volume, actual headways and wait time still count for these cities.
        when not coalesce(schedule_matchable, true) then null
        else {{ otp_band('delay_arr_sec') }}
    end as otp_band,
    schedule_relationship,
    cancelled_flag,
    skipped_flag,
    finalization_method,
    prediction_count,
    first_seen_utc,
    last_seen_utc,
    gtfs_version_id,
    source_format
from localized
