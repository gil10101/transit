with latest as (
    select city, max(gtfs_version_id) as gtfs_version_id
    from {{ source('silver', 'gtfs_static_routes') }}
    group by 1
)

select
    r.city as city_key,
    r.gtfs_version_id,
    r.route_id,
    r.agency_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    case r.route_type
        when 0 then 'tram' when 1 then 'metro' when 2 then 'rail'
        when 3 then 'bus' when 4 then 'ferry' else 'other'
    end as mode,
    r.route_color
from {{ source('silver', 'gtfs_static_routes') }} r
join latest on r.city = latest.city and r.gtfs_version_id = latest.gtfs_version_id
