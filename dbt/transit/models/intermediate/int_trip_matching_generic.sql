-- Generic RT -> static trip matching for cities without NYC's trip_id encoding
-- (P3: boston, toronto, helsinki; P3 batch 2: dc, sf, zurich). Column-compatible
-- with int_trip_matching_nyc; int_trip_matching unions the two.
--
--   * boston: RT trip_id IS the static trip_id (fixture-verified 2026-08-23)
--     -> exact equijoin, confidence 1.0. Unmatched trips are simply absent here;
--     int_stop_events_finalized left-joins, so their events still flow with
--     null schedule (OTP null) — except helsinki, whose feed also omits
--     stop_sequence, so unmatched HSL trips drop from finalized entirely.
--   * dc: same exact equijoin (fixtures 2026-08-23: rail 140/142 join static —
--     both misses UNSCHEDULED 'NR' shuttles — and bus 3635/3635). dc static is
--     TWO zips (rail + bus); dc.yaml lists both and the loader lands them under
--     ONE gtfs_version_id, so the stg_gtfs__* latest-version filter keeps both
--     modes' schedules.
--   * sf: same exact equijoin — every RT trip_id in the fixture joins the 511
--     regional static (2,494/2,494, integrator-verified 2026-08-24 against the
--     DataFeeds operator_id=RG zip). Ids are agency-namespaced ('3D:1350',
--     'SF:…'); direction_id is set on 100% of sf TUs but the matched static
--     trip supplies it downstream like every other exact city.
--   * zurich: same exact equijoin vs the Swiss NATIONAL static (fixtures
--     2026-08-23: 399/402 = 99.3%; SCHEDULED+CANCELED = 100%, only the 3 ADDED
--     runtime 'ojp:…' trips miss). The feed NEVER sets direction_id, so the
--     matched static trip supplies direction downstream (national trips.txt
--     carries the column).
--   * toronto: [rev 2026-08-25] MOVED TO THE EXACT BRANCH. The namespace
--     problem was never TTC's — we were reading the wrong one of the two GTFS
--     files TTC publishes. Against SurfaceGTFS.zip (the static that pairs with
--     the bustime realtime feed) RT trip_ids join the static at 100.0%
--     (27,942/27,953 non-ADDED on one live day) and stops at 99.6% per route,
--     so the fuzzy origin-time proxy below is no longer needed for it. The
--     fuzzy branch is kept for any future city with the same shape.
--   * toronto (HISTORICAL, wrong static): RT trip_ids shared NO namespace (2/1,478
--     joinable, review-verified 2026-08-23 — bustime ids vs CKAN schedule ids),
--     and the feed omits direction_id/start_time/start_date. Matched like a
--     no-trip-id city: (route_id, service_date, origin time) where the RT
--     origin is the earliest-stop_sequence arrival prediction, nearest static
--     origin within 5 min wins -> confidence 0.7 (prediction = schedule+delay,
--     so the tolerance absorbs origin delay). Direction comes from the static
--     trip. (sf shared this branch provisionally on 2026-08-23 and moved to
--     the exact branch a day later once its static was verified, so toronto is
--     again the only fuzzy city.)
--   * helsinki: RT trip_id is EMPTY (dictionary §B) -> resolve against static
--     via (route_id, direction_id, origin departure time) among trips active on
--     the service_date (int_service_dates), confidence 1.0. Times compare as
--     seconds so over-24h start_times ("25:05:00") match static
--     departure_seconds.
--
-- Grain: at most one row per (city_key, service_date, trip_uid) — every branch
-- rank-and-keeps-one; int_trip_matching enforces it again.

-- [rev 2026-08-25] No city needs the origin-time proxy any more: toronto, the
-- last one, moved to the exact branch when its static was corrected. The branch
-- stays wired for the next city whose RT and static share no trip namespace —
-- add it here and it works without touching the SQL below.
{% set fuzzy_cities = [] %}
{% set fuzzy_filter = "('" ~ fuzzy_cities | join("','") ~ "')" if fuzzy_cities else "('__no_fuzzy_city__')" %}

with rt_exact as (

    select distinct
        city_key,
        service_date,
        trip_id,
        trip_uid,
        route_id
    from {{ ref('stg_gtfsrt__trip_updates') }}
    where city_key in ('boston', 'dc', 'zurich', 'sf', 'toronto')
      and trip_id is not null

),

-- stg_gtfs__trips is already filtered to the latest gtfs_version_id per city;
-- the rank guards against duplicate rows within a version (fan-out insurance)
static_by_id as (

    select
        city_key,
        trip_id,
        route_id,
        direction_id,
        row_number() over (
            partition by city_key, trip_id
            order by route_id
        ) as rn
    from {{ ref('stg_gtfs__trips') }}
    where city_key in ('boston', 'dc', 'zurich', 'sf', 'toronto')

),

exact as (

    select
        r.city_key,
        r.service_date,
        r.trip_id,
        r.trip_uid,
        -- [rev 2026-08-28] the static's route is authoritative once the trip id
        -- matched: SF published an ADDED trip (SA:t_6153536_b_86615_tn_0) whose
        -- RT rows carry NO route_id even though the id exists in the 511 static.
        -- The null walked through finalized into the delivery mart and broke its
        -- not_null keys on 2026-08-27. A matched trip inherits the static route.
        coalesce(r.route_id, s.route_id) as route_id,
        r.trip_id as static_trip_id,
        s.direction_id,
        1.0 as match_confidence
    from rt_exact r
    join static_by_id s
      on s.city_key = r.city_key
     and s.trip_id = r.trip_id
     and s.rn = 1

),

-- [rev 2026-09-01] RE-KEY RECOVERY. `exact` proves a trip id exists in the
-- static; it does NOT prove the calendar places that trip on this service_date.
-- Agencies re-key trip ids when they publish a new static version, and the same
-- id then resolves to a different service pattern. The Swiss 2026-08-30 refresh
-- did exactly that: on Monday 08-31, 38,518 of 39,108 zurich RT trips id-matched
-- but only 26,397 had a schedule row, so a third of the day could not be
-- finalized (zurich is delay-only — no schedule, no event) and gold coverage
-- read 0.68. The trips were not missing from the timetable, only from that id:
-- 12,563 of them (91%) match an ACTIVE static trip on the same route with the
-- same first-stop departure time.
--
-- So a trip whose id lands on nothing the calendar runs today is re-matched by
-- (route, origin departure) against a trip the calendar DOES run today. This is
-- the fuzzy branch's logic applied to a different failure, at exact tolerance —
-- the origin time must match to the second, not within five minutes, because
-- here we are recovering a re-keyed identity rather than guessing at one.
-- Confidence 0.9: the run is certain, the id is not.
--
-- Left as a fallback rather than a replacement: an id match with no active
-- calendar row is still kept (ADDED trips legitimately look like this), it just
-- ranks below a match that a schedule can actually be joined to.
static_active as (

    select
        t.city_key,
        d.service_date,
        t.trip_id,
        t.route_id,
        t.direction_id
    from {{ ref('stg_gtfs__trips') }} t
    join {{ ref('int_service_dates') }} d
      on d.city_key = t.city_key
     and d.service_id = t.service_id
    where t.city_key in ('boston', 'dc', 'zurich', 'sf', 'toronto')

),

static_origin_seconds as (

    select
        city_key,
        trip_id,
        coalesce(departure_seconds, arrival_seconds) as origin_seconds,
        row_number() over (
            partition by city_key, trip_id
            order by stop_sequence
        ) as stop_rn
    from {{ ref('stg_gtfs__stop_times') }}
    where city_key in ('boston', 'dc', 'zurich', 'sf', 'toronto')

),

-- Active static trips with their origin departure, so the anti-join below is a
-- plain join rather than a correlated EXISTS — Snowflake rejects the latter here
-- ("Unsupported subquery type cannot be evaluated") once it carries its own join.
active_with_origin as (

    select
        a.city_key,
        a.service_date,
        a.trip_id,
        o.origin_seconds
    from static_active a
    join static_origin_seconds o
      on o.city_key = a.city_key
     and o.trip_id = a.trip_id
     and o.stop_rn = 1

),

-- RT trips whose id matched but lands on no trip the calendar runs today
rt_rekey as (

    select
        r.city_key,
        r.service_date,
        r.trip_id,
        r.trip_uid,
        r.route_id,
        cast(split_part(r.start_time, ':', 1) as integer) * 3600
      + cast(split_part(r.start_time, ':', 2) as integer) * 60
      + cast(split_part(r.start_time, ':', 3) as integer) as start_seconds
    from {{ ref('stg_gtfsrt__trip_updates') }} r
    left join active_with_origin a
      on a.city_key = r.city_key
     and a.service_date = r.service_date
     and a.trip_id = r.trip_id
     and abs(a.origin_seconds - (
           cast(split_part(r.start_time, ':', 1) as integer) * 3600
         + cast(split_part(r.start_time, ':', 2) as integer) * 60
         + cast(split_part(r.start_time, ':', 3) as integer)
     )) <= {{ var('rekey_origin_tolerance_sec') }}
    where r.city_key in ('boston', 'dc', 'zurich', 'sf', 'toronto')
      and r.trip_id is not null
      and r.route_id is not null
      and r.start_time is not null
      -- [rev 2026-09-02] Two ways an id stops meaning what it meant. It can VANISH
      -- from the calendar (the original case), or it can SURVIVE and be reused for
      -- a different service — which is worse, because the id still looks valid and
      -- the exact branch happily matches it. Zurich 2026-08-31 had 2,103 trips
      -- id-matched to a static trip departing more than half an hour from the time
      -- the feed said they started; one route's were matched to a 06:27 service
      -- while the feed still published them at 17:24, and the finalizer correctly
      -- refused predictions eleven hours staler than their own schedule. Correct,
      -- and still a loss.
      --
      -- So: an id match is only trusted when the matched trip actually departs when
      -- the feed says the trip departed. Where they disagree, identity falls back to
      -- (route, origin departure), which does not depend on ids surviving a refresh.
      -- Measured blast radius: 0 trips on 2026-08-29 and 0 on 08-30, 2,103 on the
      -- day after the refresh — the change is inert except where a rotation actually
      -- broke identity.
      -- the anti-join: no calendar-active trip with this id that also departs when
      -- the feed says this trip departed
      and a.trip_id is null

),

rekeyed as (

    select
        r.city_key,
        r.service_date,
        r.trip_id,
        r.trip_uid,
        r.route_id,
        a.trip_id as static_trip_id,
        a.direction_id,
        0.9 as match_confidence,
        row_number() over (
            partition by r.city_key, r.service_date, r.trip_uid
            order by a.trip_id
        ) as pick
    from rt_rekey r
    join static_active a
      on a.city_key = r.city_key
     and a.service_date = r.service_date
     and a.route_id = r.route_id
    join static_origin_seconds o
      on o.city_key = a.city_key
     and o.trip_id = a.trip_id
     and o.stop_rn = 1
     and o.origin_seconds = r.start_seconds

),

-- toronto: origin proxy = the earliest-stop_sequence arrival prediction
rt_fuzzy_first_stu as (

    select
        city_key,
        service_date,
        trip_id,
        trip_uid,
        route_id,
        direction_id,
        arr_pred_ts_utc,
        row_number() over (
            partition by city_key, service_date, trip_uid
            order by stop_sequence, arr_pred_ts_utc
        ) as rn
    from {{ ref('stg_gtfsrt__trip_updates') }}
    where city_key in {{ fuzzy_filter }}
      and arr_pred_ts_utc is not null
      and stop_sequence is not null

),

rt_fuzzy as (

    select
        r.city_key,
        r.service_date,
        r.trip_id,
        r.trip_uid,
        r.route_id,
        r.direction_id,
        {{ seconds_between(
            'cast(r.service_date as timestamp)',
            to_local('r.arr_pred_ts_utc', 'c.iana_tz')
        ) }} as origin_pred_seconds
    from rt_fuzzy_first_stu r
    join {{ ref('dim_city') }} c on c.city_key = r.city_key
    where r.rn = 1

),

fuzzy_origins as (

    select
        city_key,
        trip_id,
        coalesce(departure_seconds, arrival_seconds) as origin_seconds,
        row_number() over (
            partition by city_key, trip_id
            order by stop_sequence
        ) as stop_rn
    from {{ ref('stg_gtfs__stop_times') }}
    where city_key in {{ fuzzy_filter }}

),

fuzzy_static as (

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
    join fuzzy_origins o
      on o.city_key = t.city_key
     and o.trip_id = t.trip_id
     and o.stop_rn = 1
    where t.city_key in {{ fuzzy_filter }}

),

-- nearest static origin within 5 min; the tolerance absorbs origin delay.
-- The direction_id predicate stays for any future fuzzy city that states it;
-- toronto never does, so its join is route+time only, unchanged from P3
fuzzy as (

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
    from rt_fuzzy r
    join fuzzy_static s
      on s.city_key = r.city_key
     and s.service_date = r.service_date
     and s.route_id = r.route_id
     and abs(s.origin_seconds - r.origin_pred_seconds) <= 300
    where r.direction_id is null
       or s.direction_id is null
       or r.direction_id = s.direction_id

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

-- [rev 2026-09-01, second pass] A re-key match OUTRANKS an exact match here,
-- and the first version of this union had it backwards. `exact` matches on the
-- id existing in the static, ignoring whether the calendar runs that trip today
-- — which is the broken case itself. Suppressing re-keys wherever an exact
-- match existed therefore filtered out precisely the rows being recovered:
-- zurich 2026-08-31 got 442 re-keys instead of 12,563 and stayed at 0.68.
-- `rekeyed` only exists for ids the calendar does NOT run today, so a
-- calendar-active exact match can never be displaced by one.
select e.city_key, e.service_date, e.trip_id, e.trip_uid, e.route_id,
       e.static_trip_id, e.direction_id, e.match_confidence
from exact e
where not exists (
    select 1 from rekeyed r
    where r.pick = 1
      and r.city_key = e.city_key
      and r.service_date = e.service_date
      and r.trip_uid = e.trip_uid
)
union all
select r.city_key, r.service_date, r.trip_id, r.trip_uid, r.route_id,
       r.static_trip_id, r.direction_id, r.match_confidence
from rekeyed r
where r.pick = 1
union all
select city_key, service_date, trip_id, trip_uid, route_id,
       static_trip_id, direction_id, match_confidence
from fuzzy
where pick = 1
union all
select city_key, service_date, trip_id, trip_uid, route_id,
       static_trip_id, direction_id, match_confidence
from hsl
