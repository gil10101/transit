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
    {{ parse_yyyymmdd('c.start_date') }} as start_date,
    {{ parse_yyyymmdd('c.end_date') }} as end_date
from {{ source('silver', 'gtfs_static_calendar') }} c
join latest on c.city = latest.city and c.gtfs_version_id = latest.gtfs_version_id
