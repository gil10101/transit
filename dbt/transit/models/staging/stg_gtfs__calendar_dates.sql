-- [rev 2026-09-09] Every version, not just the newest — same reason as
-- stg_gtfs__calendar: service exceptions have to be read from the static that
-- was live on the day they applied to, or a refresh silently rewrites history.
-- Consumers resolve the version via int_gtfs_version_for_date.

select
    cd.city as city_key,
    cd.gtfs_version_id,
    cd.service_id,
    {{ parse_yyyymmdd('cd.date') }} as service_date,
    cd.exception_type
from {{ source('silver', 'gtfs_static_calendar_dates') }} cd
