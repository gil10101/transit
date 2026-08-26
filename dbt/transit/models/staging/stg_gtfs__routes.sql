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
    -- route_type -> mode mapping lives in macros/gtfs_mode.sql (shared with the
    -- all-versions staging behind the SCD2 dims)
    {{ gtfs_mode('r.route_type') }} as mode,
    r.route_color
from {{ source('silver', 'gtfs_static_routes') }} r
join latest on r.city = latest.city and r.gtfs_version_id = latest.gtfs_version_id
-- same national-feed filter as stg_gtfs__trips: this model supplies `mode` to the
-- activity/coverage rollups, and unfiltered it reported 5,122 Zurich routes
where {{ national_allowlist_filter('r.city', 'r.route_id') }}
