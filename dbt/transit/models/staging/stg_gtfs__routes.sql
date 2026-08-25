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
    -- base GTFS types 0-4 plus Google extended route types (HSL uses them:
    -- 701/702/704 bus, 109 suburban rail, 900 tram — 92% of its routes).
    -- zurich's Swiss national static is extended-ONLY (no 0-7 anywhere,
    -- fixture-verified 2026-08-23): 1xx rail (RT-matched routes 102-116),
    -- 700 bus, 900 tram, 1000 water -> ferry — all covered below; the static's
    -- aerial 13xx / funicular 14xx / taxi 15xx land on 'other' (marginal for
    -- city scoring; revisit only if a scored route surfaces there)
    case
        when r.route_type = 0 then 'tram'
        when r.route_type = 1 then 'metro'
        when r.route_type = 2 then 'rail'
        when r.route_type = 3 then 'bus'
        when r.route_type = 4 then 'ferry'
        when r.route_type between 100 and 199 then 'rail'
        when r.route_type between 200 and 299 then 'bus'
        when r.route_type between 400 and 499 then 'metro'
        when r.route_type between 700 and 799 then 'bus'
        when r.route_type between 900 and 999 then 'tram'
        when r.route_type between 1000 and 1299 then 'ferry'
        else 'other'
    end as mode,
    r.route_color
from {{ source('silver', 'gtfs_static_routes') }} r
join latest on r.city = latest.city and r.gtfs_version_id = latest.gtfs_version_id
-- same national-feed filter as stg_gtfs__trips: this model supplies `mode` to the
-- activity/coverage rollups, and unfiltered it reported 5,122 Zurich routes
where {{ national_allowlist_filter('r.city', 'r.route_id') }}
