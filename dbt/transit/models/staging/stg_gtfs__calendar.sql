-- [rev 2026-09-09] Every version, not just the newest. The `max(gtfs_version_id)`
-- pin that used to live here is what let a Sunday static refresh delete an
-- agency's own past: a calendar that begins at its publication date erased the
-- scheduled-trip denominator for every earlier day, on every chain, because the
-- delivery mart is full-refresh (docs/08, 2026-09-06). Consumers now pick the
-- version that was live on each service date via int_gtfs_version_for_date.
--
-- Cheap to carry: calendar is ~7k rows per version and we hold a handful of
-- versions, so this is a rounding error against the chain's cost. stop_times,
-- which is not cheap, stays pinned — see the note in int_gtfs_version_for_date.

select
    c.city as city_key,
    c.gtfs_version_id,
    c.service_id,
    c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday,
    {{ parse_yyyymmdd('c.start_date') }} as start_date,
    {{ parse_yyyymmdd('c.end_date') }} as end_date
from {{ source('silver', 'gtfs_static_calendar') }} c
