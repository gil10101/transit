-- Atomic fact: one finalized stop visit per (city_key, service_date, trip_uid,
-- stop_sequence). Local-time fields derive via dim_city.iana_tz; all analysis is
-- local, storage is UTC.
--
-- early_departure_flag (locked rule): bus departure > early_departure_grace_sec
-- early at a scheduled timepoint. Live where the static carries timepoint
-- (MBTA, HSL); false where it omits the column (NYC rail; TTC bus-defaults-none
-- per dictionary §D). Requires the P3 canonical static projection + a reparse.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['city_key', 'service_date', 'trip_uid', 'stop_sequence']
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
    sched_arr_ts_utc,
    sched_dep_ts_utc,
    actual_arr_ts_utc,
    actual_dep_ts_utc,
    delay_arr_sec,
    delay_dep_sec,
    -- locked rule: early BUS departure at a scheduled timepoint (> grace) is a
    -- service failure. timepoint is null where the static omits the column
    -- (NYC: rail anyway; TTC: bus-defaults-none per §D -> flag stays false there)
    coalesce(
        mode = 'bus' and timepoint = 1
        and delay_dep_sec < -{{ var('early_departure_grace_sec') }},
        false
    ) as early_departure_flag,
    -- a SKIPPED stop is not an on-time observation — the vehicle never served
    -- it (WMATA bus marks ~30% of STUs SKIPPED, fixtures 2026-08-23). Null band
    -- drops skips from every count(otp_band) numerator AND denominator.
    case
        when coalesce(skipped_flag, false) then null
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
