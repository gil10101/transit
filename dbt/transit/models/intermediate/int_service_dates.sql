-- Active (city, service_date, service_id) pairs, resolved from calendar + exceptions,
-- for exactly the service dates observed in the RT stream (keeps the explode bounded).
--
-- THE NEWEST-STATIC view, and deliberately so. This feeds the trip matchers and
-- int_gtfs_scheduled_stop_times, which pair these service_ids with
-- stg_gtfs__trips — also pinned to the newest version. Both sides must name the
-- same static or the join silently drops trips: resolving dates point-in-time
-- here while trips stayed newest would have deleted delay events for any recent
-- day whose service_ids were renumbered by the last refresh. The point-in-time
-- view lives in int_service_dates_pit and is used only where history matters.
--
-- [rev 2026-09-09] stg_gtfs__calendar now carries every version rather than the
-- newest (docs/08, 2026-09-06), so the pin that used to live in staging has moved
-- here. Output is unchanged from before that revision.

with observed_dates as (
    select distinct city_key, service_date
    from {{ ref('stg_gtfsrt__trip_updates') }}
),

latest as (
    select city_key, {{ static_version_pin('city_key', 'max(gtfs_version_id)') }} as gtfs_version_id
    from {{ ref('int_gtfs_versions') }}
    group by city_key
),

by_calendar as (
    select d.city_key, d.service_date, c.service_id
    from observed_dates d
    join latest l on l.city_key = d.city_key
    join {{ ref('stg_gtfs__calendar') }} c
      on c.city_key = d.city_key
     and c.gtfs_version_id = l.gtfs_version_id
     and d.service_date between c.start_date and c.end_date
     and case dayofweek(d.service_date)  -- duckdb: 0=Sunday .. 6=Saturday
             when 0 then c.sunday
             when 1 then c.monday
             when 2 then c.tuesday
             when 3 then c.wednesday
             when 4 then c.thursday
             when 5 then c.friday
             when 6 then c.saturday
         end = 1
),

added as (
    select cd.city_key, cd.service_date, cd.service_id
    from {{ ref('stg_gtfs__calendar_dates') }} cd
    join latest l on l.city_key = cd.city_key and l.gtfs_version_id = cd.gtfs_version_id
    join observed_dates d
      on d.city_key = cd.city_key and d.service_date = cd.service_date
    where cd.exception_type = 1
),

removed as (
    select cd.city_key, cd.service_date, cd.service_id
    from {{ ref('stg_gtfs__calendar_dates') }} cd
    join latest l on l.city_key = cd.city_key and l.gtfs_version_id = cd.gtfs_version_id
    where cd.exception_type = 2
)

select * from by_calendar bc
where not exists (
    select 1 from removed r
    where r.city_key = bc.city_key
      and r.service_date = bc.service_date
      and r.service_id = bc.service_id
)
union
select * from added
