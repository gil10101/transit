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
with scheduled as (

    select
        t.city_key,
        t.route_id,
        d.service_date,
        count(distinct t.trip_id) as trips_scheduled
    from {{ ref('stg_gtfs__trips') }} t
    join {{ ref('int_service_dates') }} d
      on d.city_key = t.city_key
     and d.service_id = t.service_id
    join {{ ref('dim_city') }} dc
      on dc.city_key = t.city_key
    where d.service_date <= cast({{ to_local('current_timestamp', 'dc.iana_tz') }} as date)
    group by t.city_key, t.route_id, d.service_date

),

observed as (

    select
        city_key,
        route_id,
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

cancelled as (

    select
        city_key,
        route_id,
        service_date,
        count(distinct trip_uid) as trips_cancelled
    from {{ ref('stg_gtfsrt__trip_updates') }}
    where schedule_relationship = 'CANCELED'
    group by city_key, route_id, service_date

),

-- union-then-sum keeps the full outer join cross-dialect and null-safe
unioned as (

    select city_key, route_id, service_date,
           trips_scheduled, 0 as trips_observed, 0 as trips_added,
           0 as trips_observed_scheduled, 0 as trips_cancelled
    from scheduled
    union all
    select city_key, route_id, service_date,
           0, trips_observed, trips_added, trips_observed_scheduled, 0
    from observed
    union all
    select city_key, route_id, service_date,
           0, 0, 0, 0, trips_cancelled
    from cancelled

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
        cast(sum(trips_observed_scheduled) as double)
            / nullif(sum(trips_scheduled), 0) as completeness_pct
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
    g.route_id is not null as known_coverage_gap
from rolled r
left join {{ ref('dim_city') }} c on c.city_key = r.city_key
left join {{ ref('known_coverage_gaps') }} g
       on g.city_key = r.city_key
      and g.route_id = r.route_id
{{ scd2_join(ref('dim_route'), 'dr', 'r.city_key', 'route_id', 'r.route_id', 'r.service_date') }}
