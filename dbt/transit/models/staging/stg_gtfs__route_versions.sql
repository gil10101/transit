-- ALL loaded versions of routes.txt, not just the latest — the raw material for
-- dim_route's SCD2 derivation. stg_gtfs__routes stays latest-only for the models
-- that want "the schedule as it stands"; this one exists so route history is a
-- pure function of silver.
--
-- Same national allow-list as stg_gtfs__routes: an unfiltered Zurich version row
-- set would put all 5,122 Swiss routes into the dim.

select
    r.city as city_key,
    r.gtfs_version_id,
    r.route_id,
    r.agency_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    {{ gtfs_mode('r.route_type') }} as mode,
    r.route_color
from {{ source('silver', 'gtfs_static_routes') }} r
where {{ national_allowlist_filter('r.city', 'r.route_id') }}
