with latest as (
    select city, max(gtfs_version_id) as gtfs_version_id
    from {{ source('silver', 'gtfs_static_calendar_dates') }}
    group by 1
)

select
    cd.city as city_key,
    cd.gtfs_version_id,
    cd.service_id,
    strptime(cd.date, '%Y%m%d')::date as service_date,
    cd.exception_type
from {{ source('silver', 'gtfs_static_calendar_dates') }} cd
join latest on cd.city = latest.city and cd.gtfs_version_id = latest.gtfs_version_id
