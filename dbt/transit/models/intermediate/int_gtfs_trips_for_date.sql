-- Static trips as they stood on each service date: (city, service_date, trip).
--
-- The denominator half of the point-in-time fix (docs/08, 2026-09-06).
-- int_service_dates now resolves each date's calendar against the static that was
-- live that day, which yields service_ids from older versions — and those do not
-- exist in the newest trips.txt. Measured on Helsinki: only 2,126 of an August
-- version's 6,879 service_ids survive into the September trips file. Pairing an
-- old calendar with new trips is what produced the 31% overlap I once mistook for
-- "August is unrecoverable"; paired with its OWN trips the coverage is total.
--
-- Reads the source rather than stg_gtfs__trips on purpose. That model stays
-- pinned to the newest version because the trip MATCHERS and the delay path
-- depend on it and only ever work inside the 72h reprocessing window, where the
-- newest static is correct by definition. Widening it would push every version of
-- the schedule through the matchers for no benefit and real credit cost. The
-- allowlist filter is applied identically here so the two cannot diverge on which
-- routes belong to a national feed.

select
    p.city_key,
    p.service_date,
    t.trip_id,
    t.route_id,
    t.service_id,
    t.direction_id,
    t.gtfs_version_id
from {{ ref('int_gtfs_version_for_date') }} p
join {{ source('silver', 'gtfs_static_trips') }} t
  on t.city = p.city_key
 and t.gtfs_version_id = p.gtfs_version_id
where {{ national_allowlist_filter('t.city', 't.route_id') }}
