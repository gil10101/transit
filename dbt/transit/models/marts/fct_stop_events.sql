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
    -- [rev 2026-09-10] The plausibility bound is enforced HERE as well as in the
    -- finalizer. The finalizer is incremental and never revisits rows older than
    -- the lookback, so when the bound tightened (86400 -> 43200, docs/01 §A.2)
    -- its history kept the old rule forever; re-deriving those rows would mean
    -- reprocessing all of silver. Applying the bound to whatever the finalized
    -- layer carries means one --full-refresh of THIS model (a cheap read of the
    -- int table) heals all of history, now and for any future bound change —
    -- and otp_band / early_departure_flag below inherit the bounded values by
    -- construction. Inside the 72h window both layers compute identically.
    select
        * exclude (delay_arr_sec, delay_dep_sec),
        case when abs(delay_arr_sec) <= {{ var('max_plausible_delay_sec') }}
             then delay_arr_sec end as delay_arr_sec,
        case when abs(delay_dep_sec) <= {{ var('max_plausible_delay_sec') }}
             then delay_dep_sec end as delay_dep_sec
    from {{ ref('int_stop_events_finalized') }}
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
        dr.route_key,
        ds.stop_key,
        (g.city_key is not null) as in_feed_gap,
        {{ to_local('f.actual_arr_ts_utc', 'c.iana_tz') }} as actual_arr_ts_local
    from f
    join {{ ref('dim_city') }} c on c.city_key = f.city_key
    left join {{ ref('stg_gtfs__routes') }} r
      on r.city_key = f.city_key and r.route_id = f.route_id
    -- SCD2 point-in-time FKs (P6). Natural ids stay on the fact — the keys are
    -- additive, and NULL where the static never described the route/stop on
    -- that service day.
    {{ scd2_join(ref('dim_route'), 'dr', 'f.city_key', 'route_id', 'f.route_id', 'f.service_date') }}
    {{ scd2_join(ref('dim_stop'), 'ds', 'f.city_key', 'stop_id', 'f.stop_id', 'f.service_date') }}
    left join {{ ref('int_feed_gaps') }} g
      on g.city_key = f.city_key
     and f.actual_arr_ts_utc > g.gap_start_utc
     and f.actual_arr_ts_utc < g.gap_end_utc
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
    route_key,
    direction_id,
    stop_id,
    stop_key,
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
    --
    -- [rev 2026-09-11] ...or an arrival that landed while the city's feed was dark
    -- (int_feed_gaps). When a feed dies, every in-flight trip's last prediction becomes
    -- the "actual" at each stop reached in the dark; TTC's 2026-09-10 outage put 23,474
    -- such echoes into scored gold, and the lead cap above caught 45 of them. Nothing
    -- was fetched, so nothing was observed. feed_gap_flag says which rule fired.
    coalesce(
        {{ seconds_between('last_seen_utc', 'actual_arr_ts_utc') }}
            > {{ var('max_prediction_lead_min') }} * 60,
        false
    ) or in_feed_gap as stale_observation_flag,
    in_feed_gap as feed_gap_flag,
    -- Neither a SKIPPED stop nor a stale observation is an on-time observation:
    -- the vehicle never served the stop (WMATA bus marks ~30% of STUs SKIPPED,
    -- fixtures 2026-08-23), or we never saw it do so. Null band drops both from
    -- every count(otp_band) numerator AND denominator. The rows stay, so
    -- service volume and completeness still count the trip — the same split the
    -- locked rule makes for ADDED trips.
    case
        when coalesce(skipped_flag, false) then null
        when in_feed_gap then null
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
-- [rev 2026-08-26] Drop events misdated a full service day into the past.
--
-- A stop event assigned to the PREVIOUS service day must have arrived in the small
-- hours — that is what the GTFS noon rule exists for. An event filed to yesterday that
-- arrives mid-morning is not overnight service, it is a bad service_date.
--
-- SF operators that omit start_date took the -12h default cutover, so everything
-- fetched before local noon was stamped a day early. That created a PHANTOM service
-- day: sf/2026-08-23 held 8,401 events across 433 trips, and EVERY ONE of them arrived
-- on 2026-08-24. Fixed at source (spark_jobs/timeutils.py gives sf a 4h cutover), but
-- the fix corrects new silver writes only and the historical rows persist.
--
-- Dropping rather than replaying from raw, deliberately: sf.metrics_from is 2026-08-25,
-- so BOTH 08-23 and the real 08-24 sit before scoring starts. Replaying would have
-- recovered 433 trips of morning data onto a day that is not scored either — no effect
-- on any reliability number, for real work. The rows are excluded here instead of
-- deleted from silver, so the observations survive in the replay layer if a future
-- question ever needs them.
--
-- Cutoff at hour 6 and the 1-day gap are both measured, not guessed. Across all cities:
-- sf had 7,119 previous-day rows ALL arriving 09:00-20:00, while helsinki (34,282),
-- nyc (4,980) and boston (14,128) cluster entirely in hours 0-4 and toronto had 2 in
-- 97,445. Genuine overnight service does not run at 9am.
--
-- [rev 2026-08-31] coalesce(..., false) is load-bearing, not defensive noise.
-- Without it this guard silently deleted every event that has no ARRIVAL time:
-- actual_arr_ts_local is NULL there, the comparison is NULL, `not NULL` is NULL,
-- and a WHERE keeps only TRUE. That is the origin stop of nearly every trip in
-- every city — GTFS publishes a departure and no arrival at a trip's first stop
-- — so the fact table was missing 803,125 finalized events across six cities in
-- six days (toronto 194,135, zurich 224,327, sf 202,861, dc 81,471, boston
-- 79,649, nyc 20,682), about 134k a day. It hid well: the trips still appeared
-- via their other stops, so no completeness or coverage ratio moved, and the
-- loss landed hardest on exactly the events early_departure_flag exists to
-- judge. Found 2026-08-31 by asking why 9 departure-only zurich trips reached
-- int_stop_events_finalized and not this table. A guard must drop only what it
-- can prove misdated; unknown is not a reason to delete.
where not coalesce(
    datediff('day', service_date, cast(actual_arr_ts_local as date)) = 1
    and extract(hour from actual_arr_ts_local) >= 6
, false)
