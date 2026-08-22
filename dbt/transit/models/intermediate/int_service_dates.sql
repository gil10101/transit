-- Active (city, service_date, service_id) pairs, resolved from calendar + exceptions,
-- for exactly the service dates observed in the RT stream (keeps the explode bounded).

with observed_dates as (
    select distinct city_key, service_date
    from {{ ref('stg_gtfsrt__trip_updates') }}
),

by_calendar as (
    select d.city_key, d.service_date, c.service_id
    from observed_dates d
    join {{ ref('stg_gtfs__calendar') }} c
      on c.city_key = d.city_key
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
    join observed_dates d
      on d.city_key = cd.city_key and d.service_date = cd.service_date
    where cd.exception_type = 1
),

removed as (
    select cd.city_key, cd.service_date, cd.service_id
    from {{ ref('stg_gtfs__calendar_dates') }} cd
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
