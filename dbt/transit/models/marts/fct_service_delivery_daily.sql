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
select
    r.*,
    c.metrics_from
from rolled r
left join {{ ref('dim_city') }} c on c.city_key = r.city_key
