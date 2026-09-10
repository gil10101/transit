-- Point-in-time service dates: (city, service_date, service_id) resolved against
-- the static that was LIVE on that date, not against whatever is newest.
--
-- Sibling of int_service_dates, which stays newest-pinned for the delay path. The
-- split is the whole design: the trip matchers and int_gtfs_scheduled_stop_times
-- pair service_ids with newest-version trips and only ever work inside the 72h
-- reprocessing window, where newest is correct; the delivery mart's denominator
-- covers all of history and is the only thing that needs the old statics back.
-- Keeping one model for both would either desync the delay path's joins or push
-- every version of stop_times through the matchers.
--
-- What this fixes (docs/08, 2026-09-06): agencies whose calendar.txt starts at
-- its publication date were erasing their own past on every chain, because the
-- delivery mart is full-refresh. Measured recovery in simulation before deploy —
-- judged days go Boston 10 -> 18, Helsinki 5 -> 18, Toronto 3 -> 18, and Boston
-- and Helsinki stop being permanently capped below the 20-day scorecard floor.

with observed_dates as (
    select distinct city_key, service_date
    from {{ ref('stg_gtfsrt__trip_updates') }}
),

pinned as (
    select city_key, service_date, gtfs_version_id
    from {{ ref('int_gtfs_version_for_date') }}
),

dates as (
    select d.city_key, d.service_date, p.gtfs_version_id
    from observed_dates d
    join pinned p
      on p.city_key = d.city_key and p.service_date = d.service_date
),

by_calendar as (
    select d.city_key, d.service_date, c.service_id
    from dates d
    join {{ ref('stg_gtfs__calendar') }} c
      on c.city_key = d.city_key
     and c.gtfs_version_id = d.gtfs_version_id
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
    select d.city_key, d.service_date, cd.service_id
    from dates d
    join {{ ref('stg_gtfs__calendar_dates') }} cd
      on cd.city_key = d.city_key
     and cd.gtfs_version_id = d.gtfs_version_id
     and cd.service_date = d.service_date
    where cd.exception_type = 1
),

removed as (
    select d.city_key, d.service_date, cd.service_id
    from dates d
    join {{ ref('stg_gtfs__calendar_dates') }} cd
      on cd.city_key = d.city_key
     and cd.gtfs_version_id = d.gtfs_version_id
     and cd.service_date = d.service_date
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
