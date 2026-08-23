-- P5: natural keys until SCD2 dims (P6)
--
-- Alert exposure per (city, route, service_date): distinct alerts touching the
-- route-day, overlap-deduped active minutes per alert_id, and the worst effect
-- by severity rank (alert_effect_rank macro).
--
-- Route attribution: informed_entities route_id when stated; NYC scopes service
-- alerts to trips (entity_route_id null, verified live), so the route token is
-- parsed from the RT trip_id (070950_A..S58R -> A). An alert's routes are the
-- union over ALL its versions (MTA re-issues alerts with empty entity lists;
-- those versions must not dilute to unattributed). Only alerts that never name
-- a route in any version land on '_unattributed' rather than vanishing.
--
-- Active periods: NYC alerts state none ('[]'), so a stated period is
-- coalesced with the alert's observed window (first..last fetched_at of any of
-- its versions). Minutes clamp to the local calendar day of the row's
-- service_date; the post-midnight tail of an overnight service_date is
-- deliberately not attributed (bounded error, revisit with P6 scoring).

with alert_versions as (

    select
        city_key,
        service_date,
        alert_id,
        effect,
        fetched_at,
        period_start_ts_utc,
        period_end_ts_utc,
        coalesce(
            entity_route_id,
            case when city_key = 'nyc'
                 then {{ re_extract('entity_trip_id', '^[0-9]{6}_([A-Za-z0-9]+)\.\.', 1) }}
            end
        ) as route_id
    from {{ ref('stg_gtfsrt__alerts') }}

),

-- union of routes an alert ever names, across versions and entities
alert_routes as (

    select distinct city_key, alert_id, route_id
    from alert_versions
    where route_id is not null

),

-- observed window per alert: fallback when the feed states no active period
observed_windows as (

    select
        city_key,
        alert_id,
        min(fetched_at) as first_seen_utc,
        max(fetched_at) as last_seen_utc
    from alert_versions
    group by city_key, alert_id

),

periods as (

    select distinct
        v.city_key,
        v.service_date,
        coalesce(r.route_id, '_unattributed') as route_id,
        v.alert_id,
        coalesce(v.period_start_ts_utc, w.first_seen_utc) as p_start_utc,
        coalesce(v.period_end_ts_utc, w.last_seen_utc) as p_end_utc
    from alert_versions v
    join observed_windows w
      on w.city_key = v.city_key
     and w.alert_id = v.alert_id
    left join alert_routes r
      on r.city_key = v.city_key
     and r.alert_id = v.alert_id

),

localized as (

    select
        p.city_key,
        p.service_date,
        p.route_id,
        p.alert_id,
        greatest(
            {{ to_local('p.p_start_utc', 'c.iana_tz') }},
            cast(p.service_date as timestamp)
        ) as s_local,
        least(
            {{ to_local('p.p_end_utc', 'c.iana_tz') }},
            {{ dbt.dateadd('day', 1, 'cast(p.service_date as timestamp)') }}
        ) as e_local
    from periods p
    join {{ ref('dim_city') }} c on c.city_key = p.city_key

),

clamped as (

    -- >= keeps zero-length windows (alert seen once): 0 minutes, still active
    select * from localized where e_local >= s_local

),

-- classic island merge: a row starts a new island when it opens after every
-- earlier period in the (route-day, alert) group has closed
flagged as (

    select
        *,
        case
            when max(e_local) over (
                     partition by city_key, service_date, route_id, alert_id
                     order by s_local, e_local
                     rows between unbounded preceding and 1 preceding
                 ) is null then 1
            when s_local > max(e_local) over (
                     partition by city_key, service_date, route_id, alert_id
                     order by s_local, e_local
                     rows between unbounded preceding and 1 preceding
                 ) then 1
            else 0
        end as new_island
    from clamped

),

islands as (

    select
        *,
        sum(new_island) over (
            partition by city_key, service_date, route_id, alert_id
            order by s_local, e_local
            rows between unbounded preceding and current row
        ) as island_id
    from flagged

),

merged as (

    select
        city_key,
        service_date,
        route_id,
        alert_id,
        {{ seconds_between('min(s_local)', 'max(e_local)') }} / 60.0 as island_minutes
    from islands
    group by city_key, service_date, route_id, alert_id, island_id

),

per_alert as (

    select
        city_key,
        service_date,
        route_id,
        alert_id,
        sum(island_minutes) as alert_minutes
    from merged
    group by city_key, service_date, route_id, alert_id

),

worst_effect as (

    select
        city_key,
        service_date,
        route_id,
        effect as worst_effect
    from (
        select distinct
            v.city_key,
            v.service_date,
            coalesce(r.route_id, '_unattributed') as route_id,
            coalesce(v.effect, 'UNKNOWN_EFFECT') as effect
        from alert_versions v
        left join alert_routes r
          on r.city_key = v.city_key
         and r.alert_id = v.alert_id
    ) e
    qualify row_number() over (
        partition by city_key, service_date, route_id
        order by {{ alert_effect_rank('effect') }}, effect
    ) = 1

)

select
    md5(concat_ws('|', a.city_key, a.route_id, a.service_date)) as alert_day_key,
    a.city_key,
    a.route_id,
    a.service_date,
    count(distinct a.alert_id) as alerts_active,
    round(sum(a.alert_minutes), 1) as alert_minutes,
    max(w.worst_effect) as worst_effect
from per_alert a
join worst_effect w
  on w.city_key = a.city_key
 and w.service_date = a.service_date
 and w.route_id = a.route_id
group by a.city_key, a.route_id, a.service_date
