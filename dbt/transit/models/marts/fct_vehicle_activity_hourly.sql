-- P5: natural keys until SCD2 dims (P6)
--
-- Vehicles/trips/routes active per (city, mode, service_date, local_hour), from
-- vehicle positions. mode joins through stg_gtfs__routes (route_type-derived,
-- the docs/02 taxonomy: metro/tram/bus/rail/ferry) — dim_city carries no mode
-- column. NYC subway VP has no vehicle_id, so distinct_vehicles falls back to
-- distinct trip_uid and vehicle_id_reliable=false marks the group as a proxy
-- count (docs/02 fct_vehicle_activity_hourly).

with pings as (

    select
        v.city_key,
        v.service_date,
        v.trip_uid,
        v.route_id,
        v.vehicle_id,
        extract(hour from {{ to_local('v.ts_utc', 'c.iana_tz') }}) as local_hour
    from {{ ref('stg_gtfsrt__vehicle_positions') }} v
    join {{ ref('dim_city') }} c on c.city_key = v.city_key
    where v.ts_utc is not null

),

with_mode as (

    select
        p.*,
        coalesce(r.mode, 'unknown') as mode
    from pings p
    left join {{ ref('stg_gtfs__routes') }} r
      on r.city_key = p.city_key
     and r.route_id = p.route_id

)

select
    md5(concat_ws('|', city_key, mode, service_date, local_hour)) as activity_key,
    city_key,
    mode,
    service_date,
    local_hour,
    case
        when max(case when vehicle_id is not null then 1 else 0 end) = 1
            then count(distinct vehicle_id)
        else count(distinct trip_uid)
    end as distinct_vehicles,
    count(distinct trip_uid) as distinct_trips_active,
    count(distinct route_id) as distinct_routes_active,
    max(case when vehicle_id is not null then 1 else 0 end) = 1 as vehicle_id_reliable
from with_mode
group by city_key, mode, service_date, local_hour
