-- Latest static GTFS version per city. NYC static trip_ids embed the same
-- origin-time token RT uses: <schedule>_<HHMMSS-centimin>_<route>..<dir><track>.

with latest as (
    select city, max(gtfs_version_id) as gtfs_version_id
    from {{ source('silver', 'gtfs_static_trips') }}
    group by 1
)

select
    t.city as city_key,
    t.gtfs_version_id,
    t.trip_id,
    t.route_id,
    t.service_id,
    t.direction_id,
    t.trip_headsign,
    t.shape_id,
    regexp_extract(t.trip_id, '([0-9]{6}_.+)$', 1) as origin_time_token
from {{ source('silver', 'gtfs_static_trips') }} t
join latest on t.city = latest.city and t.gtfs_version_id = latest.gtfs_version_id
