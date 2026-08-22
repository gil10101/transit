-- Atomic fact: one finalized stop visit per (city_key, service_date, trip_uid,
-- stop_sequence). Local-time fields derive via dim_city.iana_tz; all analysis is
-- local, storage is UTC. NYC is subway-only, so the bus-timepoint early-departure
-- rule never fires here (kept as an explicit false until bus cities land).

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
        {{ to_local('f.actual_arr_ts_utc', 'c.iana_tz') }} as actual_arr_ts_local
    from f
    join {{ ref('dim_city') }} c on c.city_key = f.city_key
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
    false as early_departure_flag,
    {{ otp_band('delay_arr_sec') }} as otp_band,
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
