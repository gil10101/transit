-- Point-in-time scheduled stop times: the schedule exploded against the static
-- that was LIVE on each service date, rather than the newest one.
--
-- WHY THIS EXISTS (2026-09-14). int_gtfs_scheduled_stop_times is pinned to
-- max(gtfs_version_id) per city, which int_gtfs_version_for_date explains is
-- correct for its purpose: it serves the 72h reprocessing window, where the
-- newest static always covers the date. The hazard is that a --full-refresh of
-- fct_headways then recomputes ALL of history against that newest static, and
-- agencies whose calendar.txt begins at the publication date no longer cover
-- their own past. Measured lookback: helsinki 1-5 days, boston 7-13,
-- toronto 0-39. A full refresh on 2026-09-14 took helsinki from 4,010 rated
-- route-days to 0 and dc from 2,232 to 24 in the 09-01..09-09 window alone.
--
-- This model is the repair. It is NOT part of the steady-state chain: it builds
-- only for the cities named in var('pit_cities') and is empty otherwise, so the
-- default path keeps its cost profile. Re-scanning every version of stop_times
-- (329M rows, the largest table in the schedule layer) on every chain would cost
-- far more than the problem is worth — but once, to recover deleted history, it
-- is exactly worth it.
--
-- The trips half is already solved: int_gtfs_trips_for_date pairs each date's
-- version with that version's own trips.txt, and int_service_dates_pit resolves
-- which service_ids actually run. Pairing an old calendar with new trips is what
-- produced the 31% overlap once mistaken for "August is unrecoverable"; paired
-- with its own trips the coverage is total. This model adds the third leg:
-- that version's own stop_times.

{% set pit_cities = var('pit_cities', '') %}
{% set pit_list = pit_cities.split(',') | map('trim') | reject('equalto', '') | list %}

with active_trips as (

    select
        t.city_key,
        t.service_date,
        t.trip_id,
        t.route_id,
        t.direction_id,
        t.gtfs_version_id
    from {{ ref('int_gtfs_trips_for_date') }} t
    join {{ ref('int_service_dates_pit') }} d
      on d.city_key = t.city_key
     and d.service_id = t.service_id
     and d.service_date = t.service_date
    {% if pit_list %}
    where t.city_key in ({{ "'" ~ pit_list | join("','") ~ "'" }})
    {% else %}
    -- no cities selected: build empty rather than scanning every static version
    where 1 = 0
    {% endif %}

)

select
    a.city_key,
    a.service_date,
    a.trip_id,
    a.route_id,
    a.direction_id,
    st.stop_id,
    st.stop_sequence,
    {% if target.type == 'snowflake' %}st.timepoint{% else %}cast(null as int) as timepoint{% endif %},
    a.gtfs_version_id,
    {{ scheduled_ts_utc('a.service_date', 'st.arrival_seconds', 'c.iana_tz') }} as sched_arr_ts_utc,
    {{ scheduled_ts_utc('a.service_date', 'st.departure_seconds', 'c.iana_tz') }} as sched_dep_ts_utc
from active_trips a
join {{ source('silver', 'gtfs_static_stop_times') }} st
  on st.city = a.city_key
 and st.gtfs_version_id = a.gtfs_version_id
 and st.trip_id = a.trip_id
join {{ ref('dim_city') }} c
  on c.city_key = a.city_key
