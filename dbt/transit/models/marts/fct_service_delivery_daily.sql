-- P5: natural keys until SCD2 dims (P6)
--
-- Daily service delivery per (city, route, service_date):
--   trips_scheduled  static trips active on the service_date (int_service_dates)
--   trips_observed   distinct trip_uid with any finalized stop event
--   trips_added      observed with schedule_relationship ADDED
--   trips_cancelled  from trip updates (CANCELED trips carry no stop events,
--                    so fct_stop_events never sees them — count at the feed)
--   completeness_pct observed non-ADDED / scheduled — the killed-feed tripwire
--                    reads this dropping toward 0.

-- [rev 2026-08-25] Bounded to service days that have actually STARTED in the city's
-- own local time. int_service_dates expands the whole published calendar, which runs
-- weeks ahead, so without this the mart carried future-dated rows (helsinki out to
-- 2026-08-26 while it was still the 25th) showing 100% of trips "not delivered".
-- The completeness tests never saw them — service_day_closed already excluded them —
-- but a fact table that asserts tomorrow's service was missed is wrong on its face and
-- silently poisons any chart or average built on the raw mart.
-- the schedule universe at TRIP grain: every (city, service_date, trip) the
-- static calendar says runs, up to the city's local today. Both the scheduled
-- denominator and the cancelled numerator draw from THIS set, which is what
-- makes cancelled <= scheduled an invariant instead of a hope.
with active_trips as (

    select
        t.city_key,
        t.route_id,
        d.service_date,
        t.trip_id
    from {{ ref('stg_gtfs__trips') }} t
    join {{ ref('int_service_dates') }} d
      on d.city_key = t.city_key
     and d.service_id = t.service_id
    join {{ ref('dim_city') }} dc
      on dc.city_key = t.city_key
    where d.service_date <= cast({{ to_local('current_timestamp', 'dc.iana_tz') }} as date)

),

scheduled as (

    select
        city_key,
        route_id,
        service_date,
        count(distinct trip_id) as trips_scheduled
    from active_trips
    group by city_key, route_id, service_date

),

observed as (

    -- [rev 2026-08-28] the route grain must survive events with no route at all:
    -- an ADDED trip that never matched static and whose feed rows omit route_id
    -- (matched ones now inherit the static route upstream). Those land in an
    -- explicit __unrouted__ bucket instead of a NULL key that breaks
    -- delivery_key/route_id not_null — visible, counted, never joined to dims
    -- (route_key stays NULL by construction).
    select
        city_key,
        coalesce(route_id, '__unrouted__') as route_id,
        service_date,
        count(distinct trip_uid) as trips_observed,
        count(distinct case
            when coalesce(schedule_relationship, 'SCHEDULED') = 'ADDED' then trip_uid
        end) as trips_added,
        count(distinct case
            when coalesce(schedule_relationship, 'SCHEDULED') <> 'ADDED' then trip_uid
        end) as trips_observed_scheduled
    from {{ ref('fct_stop_events') }}
    group by city_key, route_id, service_date

),

published as (

    -- [rev 2026-08-31] The column this mart was missing. Until now it could say
    -- how much scheduled service we SCORED, but not whether the shortfall was the
    -- agency publishing nothing or us losing what it published — and that single
    -- missing distinction is what forced every coverage question into a
    -- hand-maintained seed list. trips_published counts the trips the feed
    -- actually gave us something usable for (a delay or a timestamp; a row with
    -- neither cannot be placed at a stop, so counting it would overstate what was
    -- available). trips_observed / trips_published is then the capture rate: the
    -- only one of these ratios that is about US.
    select
        city_key,
        coalesce(route_id, '__unrouted__') as route_id,
        service_date,
        count(distinct trip_uid) as trips_published
    from {{ ref('stg_gtfsrt__trip_updates') }}
    where arr_delay_sec is not null
       or dep_delay_sec is not null
       or arr_pred_ts_utc is not null
       or dep_pred_ts_utc is not null
    group by city_key, route_id, service_date

),

cancelled as (

    -- [rev 2026-08-27] Two counts, two universes, kept apart BY CONSTRUCTION.
    -- The feed cancels trips from ITS schedule universe; our denominator is the
    -- latest loaded static. During a Swiss service change, 91-E had 95 CANCELED
    -- trip ids against 90 scheduled — the ratio broke 100% and three chains went
    -- red. The first response widened the range test; that was a patch and was
    -- reverted. The fix: trips_cancelled counts only cancels whose trip MATCHES
    -- our schedule (int_trip_matching is the RT<->static bridge every city
    -- already goes through, including HSL's empty trip_ids), so cancel_pct is a
    -- true fraction of the denominator and can NEVER exceed 1. Cancels of trips
    -- outside our schedule stay visible in trips_cancelled_unscheduled — feed
    -- truth, not thrown away, just never divided by the wrong denominator.
    select
        c.city_key,
        coalesce(c.route_id, '__unrouted__') as route_id,
        c.service_date,
        -- distinct STATIC trips ACTIVE THAT DATE, not RT uids and not merely
        -- id-matched trips: the matcher proves identity, active_trips proves the
        -- calendar ran it that day (the 91-E cancels id-matched 95 static trips
        -- of which only 90 were calendar-active — a service change cancelling
        -- other days' ids). Numerator drawn from the denominator's own set:
        -- cancelled <= scheduled by construction.
        count(distinct a.trip_id) as trips_cancelled,
        count(distinct case when a.trip_id is null then c.trip_uid end)
            as trips_cancelled_unscheduled
    from {{ ref('stg_gtfsrt__trip_updates') }} c
    left join {{ ref('int_trip_matching') }} m
      on m.city_key = c.city_key
     and m.service_date = c.service_date
     and m.trip_uid = c.trip_uid
    left join active_trips a
      on a.city_key = c.city_key
     and a.service_date = c.service_date
     and a.trip_id = m.static_trip_id
    where c.schedule_relationship = 'CANCELED'
    group by c.city_key, c.route_id, c.service_date

),

-- union-then-sum keeps the full outer join cross-dialect and null-safe
unioned as (

    select city_key, route_id, service_date,
           trips_scheduled, 0 as trips_observed, 0 as trips_added,
           0 as trips_observed_scheduled, 0 as trips_cancelled,
           0 as trips_cancelled_unscheduled, 0 as trips_published
    from scheduled
    union all
    select city_key, route_id, service_date,
           0, trips_observed, trips_added, trips_observed_scheduled, 0, 0, 0
    from observed
    union all
    select city_key, route_id, service_date,
           0, 0, 0, 0, trips_cancelled, trips_cancelled_unscheduled, 0
    from cancelled
    union all
    select city_key, route_id, service_date,
           0, 0, 0, 0, 0, 0, trips_published
    from published

),

rolled as (

    select
        md5(concat_ws('|', city_key, route_id, service_date)) as delivery_key,
        city_key,
        route_id,
        service_date,
        sum(trips_scheduled) as trips_scheduled,
        sum(trips_observed) as trips_observed,
        sum(trips_added) as trips_added,
        sum(trips_cancelled) as trips_cancelled,
        sum(trips_cancelled_unscheduled) as trips_cancelled_unscheduled,
        sum(trips_published) as trips_published,
        cast(sum(trips_observed_scheduled) as double)
            / nullif(sum(trips_scheduled), 0) as completeness_pct,
        -- capture_rate answers the only question we can act on: of what the feed
        -- published, how much did we finalize? completeness_pct mixes that with
        -- how much the agency chose to publish at all.
        cast(sum(trips_observed) as double)
            / nullif(sum(trips_published), 0) as capture_rate
    from unioned
    group by city_key, route_id, service_date

)

-- metrics_from rides along so the completeness tests can judge each city only
-- from its own first FULL service day. dbt's generic-test parser refuses ref()
-- inside a test's `where`, so the floor has to be a column on the fact.
--
-- service_day_closed is the ceiling, and it has to be LOCAL. The tests used to
-- gate on `service_date < current_date`, which is UTC: between 00:00 and ~07:00
-- UTC that makes the current New York / Toronto service day look closed while
-- it still has hours to run, so every route fails a completeness floor it was
-- never meant to be judged against (157 route-days, 2026-08-25 00:20Z). A GTFS
-- service day also runs past local midnight, so the day is only closed once
-- local time is 3h into the next date.
-- known_coverage_gap marks a route the AGENCY does not publish realtime for at all, so
-- no work on our side can raise its completeness. It rides along as a column for the
-- same reason metrics_from does: dbt's generic-test parser refuses ref() in a test's
-- `where`. The error-severity test excludes these; the warn-severity one does NOT, so
-- they stay visible rather than disappearing.
--
-- [rev 2026-08-25] Only gaps CONFIRMED against the agency's own docs go in the seed.
-- Helsinki's tram 100H and several of its bus routes also sit below the floor and are
-- deliberately NOT listed: we do not yet know why they are missing, and hiding an
-- unexplained gap is how a warehouse ends up all-green and wrong.
select
    r.*,
    dr.route_key,
    c.metrics_from,
    cast(
        {{ to_local('current_timestamp', 'c.iana_tz') }} - interval '3 hour' as date
    ) > r.service_date as service_day_closed,
    g.route_id is not null as known_coverage_gap,
    -- dated our-outage marker (seed incident_days): the executable form of a
    -- docs/08 incident entry. Only the error tripwire reads it.
    i.city_key is not null as incident_day
from rolled r
left join {{ ref('dim_city') }} c on c.city_key = r.city_key
left join {{ ref('known_coverage_gaps') }} g
       on g.city_key = r.city_key
      and g.route_id = r.route_id
left join {{ ref('incident_days') }} i
       on i.city_key = r.city_key
      and i.service_date = r.service_date
{{ scd2_join(ref('dim_route'), 'dr', 'r.city_key', 'route_id', 'r.route_id', 'r.service_date') }}
