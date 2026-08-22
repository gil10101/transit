with latest as (
    select city, max(gtfs_version_id) as gtfs_version_id
    from {{ source('silver', 'gtfs_static_calendar') }}
    group by 1
)

select
    c.city as city_key,
    c.gtfs_version_id,
    c.service_id,
    c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday,
    strptime(c.start_date, '%Y%m%d')::date as start_date,
    strptime(c.end_date, '%Y%m%d')::date as end_date
from {{ source('silver', 'gtfs_static_calendar') }} c
join latest on c.city = latest.city and c.gtfs_version_id = latest.gtfs_version_id
