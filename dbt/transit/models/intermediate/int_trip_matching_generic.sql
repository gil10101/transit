-- Generic RT -> static trip matching for cities without NYC's trip_id encoding
-- (P3: boston, toronto, helsinki). Column-compatible with int_trip_matching_nyc;
-- int_trip_matching unions the two.
--
--   * boston: RT trip_id IS the static trip_id (fixture-verified 2026-08-23)
--     -> exact equijoin, confidence 1.0. Unmatched trips are simply absent here;
--     int_stop_events_finalized left-joins, so their events still flow with
--     null schedule (OTP null) — except helsinki, whose feed also omits
--     stop_sequence, so unmatched HSL trips drop from finalized entirely.
--   * toronto: RT trip_ids share NO namespace with the static zip (2/1,478
--     joinable, review-verified 2026-08-23 — bustime ids vs CKAN schedule ids),
--     and the feed omits direction_id/start_time/start_date. Matched like a
--     no-trip-id city: (route_id, service_date, origin time) where the RT
--     origin is the earliest-stop_sequence arrival prediction, nearest static
--     origin within 5 min wins -> confidence 0.7 (prediction = schedule+delay,
--     so the tolerance absorbs origin delay). Direction comes from the static
--     trip.
--   * helsinki: RT trip_id is EMPTY (dictionary §B) -> resolve against static
--     via (route_id, direction_id, origin departure time) among trips active on
--     the service_date (int_service_dates), confidence 1.0. Times compare as
--     seconds so over-24h start_times ("25:05:00") match static
--     departure_seconds.
--
-- Grain: at most one row per (city_key, service_date, trip_uid) — every branch
-- rank-and-keeps-one; int_trip_matching enforces it again.

with rt_exact as (

    select distinct
        city_key,
        service_date,
        trip_id,
        trip_uid,
        route_id
    from {{ ref('stg_gtfsrt__trip_updates') }}
    where city_key = 'boston'
      and trip_id is not null

),

-- stg_gtfs__trips is already filtered to the latest gtfs_version_id per city;
-- the rank guards against duplicate rows within a version (fan-out insurance)
static_by_id as (

    select
        city_key,
        trip_id,
        direction_id,
        row_number() over (
            partition by city_key, trip_id
            order by route_id
        ) as rn
    from {{ ref('stg_gtfs__trips') }}
    where city_key = 'boston'

),

exact as (

    select
        r.city_key,
        r.service_date,
        r.trip_id,
        r.trip_uid,
        r.route_id,
        r.trip_id as static_trip_id,
        s.direction_id,
        1.0 as match_confidence
    from rt_exact r
    join static_by_id s
      on s.city_key = r.city_key
     and s.trip_id = r.trip_id
     and s.rn = 1

),

-- toronto: origin proxy = the earliest-stop_sequence arrival prediction
rt_toronto_first_stu as (

    select
        city_key,
        service_date,
        trip_id,
        trip_uid,
        route_id,
        arr_pred_ts_utc,
        row_number() over (
            partition by city_key, service_date, trip_uid
            order by stop_sequence, arr_pred_ts_utc
        ) as rn
    from {{ ref('stg_gtfsrt__trip_updates') }}
    where city_key = 'toronto'
      and arr_pred_ts_utc is not null
      and stop_sequence is not null

),

rt_toronto as (

    select
        r.city_key,
        r.service_date,
        r.trip_id,
        r.trip_uid,
        r.route_id,
        {{ seconds_between(
            'cast(r.service_date as timestamp)',
            to_local('r.arr_pred_ts_utc', 'c.iana_tz')
        ) }} as origin_pred_seconds
    from rt_toronto_first_stu r
    join {{ ref('dim_city') }} c on c.city_key = r.city_key
    where r.rn = 1

),

tor_origins as (

    select
        city_key,
        trip_id,
        coalesce(departure_seconds, arrival_seconds) as origin_seconds,
        row_number() over (
            partition by city_key, trip_id
            order by stop_sequence
        ) as stop_rn
    from {{ ref('stg_gtfs__stop_times') }}
    where city_key = 'toronto'

),

tor_static as (

    select
        t.city_key,
        d.service_date,
        t.trip_id as static_trip_id,
        t.route_id,
        t.direction_id,
        o.origin_seconds
    from {{ ref('stg_gtfs__trips') }} t
    join {{ ref('int_service_dates') }} d
      on d.city_key = t.city_key
     and d.service_id = t.service_id
    join tor_origins o
      on o.city_key = t.city_key
     and o.trip_id = t.trip_id
     and o.stop_rn = 1
    where t.city_key = 'toronto'

),

-- nearest static origin within 5 min; the tolerance absorbs origin delay
toronto as (

    select
        r.city_key,
        r.service_date,
        r.trip_id,
        r.trip_uid,
        r.route_id,
        s.static_trip_id,
        s.direction_id,
        0.7 as match_confidence,
        row_number() over (
            partition by r.city_key, r.service_date, r.trip_uid
            order by abs(s.origin_seconds - r.origin_pred_seconds), s.static_trip_id
        ) as pick
    from rt_toronto r
    join tor_static s
      on s.city_key = r.city_key
     and s.service_date = r.service_date
     and s.route_id = r.route_id
     and abs(s.origin_seconds - r.origin_pred_seconds) <= 300

),

rt_hsl as (

    select distinct
        city_key,
        service_date,
        trip_id,
        trip_uid,
        route_id,
        direction_id,
        cast(split_part(start_time, ':', 1) as integer) * 3600
      + cast(split_part(start_time, ':', 2) as integer) * 60
      + cast(split_part(start_time, ':', 3) as integer) as start_seconds
    from {{ ref('stg_gtfsrt__trip_updates') }}
    where city_key = 'helsinki'
      and route_id is not null
      and direction_id is not null
      and start_time is not null

),

-- origin departure per static trip = first stop_sequence's departure time
hsl_origins as (

    select
        city_key,
        trip_id,
        coalesce(departure_seconds, arrival_seconds) as origin_seconds,
        row_number() over (
            partition by city_key, trip_id
            order by stop_sequence
        ) as stop_rn
    from {{ ref('stg_gtfs__stop_times') }}
    where city_key = 'helsinki'

),

hsl_static as (

    select
        t.city_key,
        d.service_date,
        t.trip_id as static_trip_id,
        t.route_id,
        t.direction_id,
        o.origin_seconds,
        row_number() over (
            partition by t.city_key, d.service_date, t.route_id, t.direction_id, o.origin_seconds
            order by t.trip_id
        ) as rn
    from {{ ref('stg_gtfs__trips') }} t
    join {{ ref('int_service_dates') }} d
      on d.city_key = t.city_key
     and d.service_id = t.service_id
    join hsl_origins o
      on o.city_key = t.city_key
     and o.trip_id = t.trip_id
     and o.stop_rn = 1
    where t.city_key = 'helsinki'

),

hsl as (

    select
        r.city_key,
        r.service_date,
        r.trip_id,
        r.trip_uid,
        r.route_id,
        s.static_trip_id,
        s.direction_id,
        1.0 as match_confidence
    from rt_hsl r
    join hsl_static s
      on s.city_key = r.city_key
     and s.service_date = r.service_date
     and s.route_id = r.route_id
     and s.direction_id = r.direction_id
     and s.origin_seconds = r.start_seconds
     and s.rn = 1

)

select city_key, service_date, trip_id, trip_uid, route_id,
       static_trip_id, direction_id, match_confidence
from exact
union all
select city_key, service_date, trip_id, trip_uid, route_id,
       static_trip_id, direction_id, match_confidence
from toronto
where pick = 1
union all
select city_key, service_date, trip_id, trip_uid, route_id,
       static_trip_id, direction_id, match_confidence
from hsl
