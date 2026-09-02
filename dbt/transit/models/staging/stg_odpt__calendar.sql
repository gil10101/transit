-- Latest ODPT calendar dump (tokyo): the generic Weekday/Saturday/Holiday/
-- SaturdayHoliday family plus operator-specific ones. TrainTimetable rows key
-- their operating days on calendar_urn; the day-type resolution for a concrete
-- service_date happens in int_odpt_stop_events.

with latest as (
    select city, max(odpt_version_id) as odpt_version_id
    from {{ source('silver', 'odpt_calendar') }}
    group by 1
)

select
    c.city as city_key,
    c.odpt_version_id,
    c.calendar_urn,
    c.title_ja,
    c.title_en
from {{ source('silver', 'odpt_calendar') }} c
join latest on c.city = latest.city and c.odpt_version_id = latest.odpt_version_id
