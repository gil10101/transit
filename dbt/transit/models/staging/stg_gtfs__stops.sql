with latest as (
    select city, max(gtfs_version_id) as gtfs_version_id
    from {{ source('silver', 'gtfs_static_stops') }}
    group by 1
)

select
    s.city as city_key,
    s.gtfs_version_id,
    s.stop_id,
    s.stop_code,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    s.parent_station,
    s.location_type
from {{ source('silver', 'gtfs_static_stops') }} s
join latest on s.city = latest.city and s.gtfs_version_id = latest.gtfs_version_id
