-- Latest static GTFS version per city. NYC static trip_ids embed the same
-- origin-time token RT uses: <schedule>_<HHMMSS-centimin>_<route>..<dir><track>.

with latest as (
    select city, {{ static_version_pin('city', 'max(gtfs_version_id)') }} as gtfs_version_id
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
    {{ re_extract('t.trip_id', '([0-9]{6}_.+)$', 1) }} as origin_time_token
from {{ source('silver', 'gtfs_static_trips') }} t
join latest on t.city = latest.city and t.gtfs_version_id = latest.gtfs_version_id
-- national-feed cities: keep only routes that serve the city we score. Trips are the
-- choke point for the whole schedule side (stop_times, service dates and frequency all
-- reach the schedule through a trip), so filtering here bounds every downstream model.
where {{ national_allowlist_filter('t.city', 't.route_id') }}
