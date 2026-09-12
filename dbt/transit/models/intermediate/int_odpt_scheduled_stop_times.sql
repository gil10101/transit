-- Tokyo's scheduled stop times, at the grain fct_headways and
-- int_service_frequency need: (city, service_date, route, direction, stop) with
-- sched_arr_ts_utc.
--
-- WHY THIS MODEL EXISTS. Both of those models take their scheduled side from
-- int_gtfs_scheduled_stop_times, which is static GTFS trips joined to
-- int_service_dates. Tokyo rail has no GTFS-RT and never enters that path --
-- its schedule is odpt:TrainTimetable -- so it contributed zero scheduled stop
-- times and the consequences were silent rather than loud:
--
--   * fct_headways computed 362,473 real Tokyo gaps with sched_headway_sec NULL
--     on every one, so gap_ratio, bunched_flag and big_gap_flag -- all of which
--     divide by it -- nulled out together.
--   * int_service_frequency produced no Tokyo rows at all, so is_frequent was
--     unknown, and EWT only scores frequent slices, so excess wait never ran.
--
-- Tokyo therefore showed a dash for bunching and excess wait while carrying a
-- full OTP column, which reads as "this city cannot be measured" when the truth
-- was "its timetable was never joined" (found 2026-09-12). The arrivals were
-- always there; only the schedule to compare them against was missing.
--
-- Day-type resolution and the calendar priority are deliberately identical to
-- int_odpt_stop_events and int_odpt_scheduled_trips, so the observed side, the
-- scheduled side and the completeness denominator agree about what runs on a
-- given date. Same KNOWN GAP, too: a Japanese national holiday falling on a
-- weekday runs a holiday schedule but classifies as Weekday here (docs/01
-- §C.4). None falls between Tokyo's first data (2026-09-03) and the 2026-09-17
-- close -- the next are the 21st and 23rd.
--
-- Toei only, because int_odpt_stop_map is Toei only (Metro publishes no
-- odpt:Train). NipporiToneri is in the map but has no realtime at all, so it
-- contributes scheduled arrivals that pair with no observed ones -- consumers
-- left join, and that is the honest reading: service we know was scheduled and
-- never saw.
--
-- ARRIVAL TIMES ARE NOT COALESCED WITH DEPARTURES. ODPT omits arrival_time at a
-- train's origin station (a train does not arrive where it begins), so those
-- stops carry departure only and yield a NULL sched_arr_ts_utc here. That is
-- deliberate parity with int_odpt_stop_events, which sets actual_arr from
-- arrival_seconds and so emits no observed arrival at an origin either --
-- inventing a scheduled arrival for a stop that can never produce an observed
-- one would add a scheduled gap that pairs with nothing and drag the median
-- headway down.

with dates as (

    -- bounded to service dates Tokyo actually produced realtime for, mirroring
    -- int_odpt_scheduled_trips so the scheduled side never asserts tomorrow
    select distinct city_key, service_date
    from {{ ref('stg_odpt__trains') }}

),

railway_dirs as (

    select distinct railway_urn, ascending_direction, descending_direction
    from {{ ref('stg_odpt__railway') }}

),

timetables as (

    select distinct
        timetable_urn,
        railway_urn,
        train_number,
        calendar_urn,
        rail_direction_urn
    from {{ ref('stg_odpt__train_timetable') }}

),

active as (

    select
        d.city_key,
        d.service_date,
        t.timetable_urn,
        t.railway_urn,
        t.train_number,
        t.rail_direction_urn,
        t.calendar_urn,
        {% if target.type == 'snowflake' %}
        dayofweekiso(d.service_date) as iso_dow
        {% else %}
        isodow(d.service_date) as iso_dow
        {% endif %}
    from dates d
    join timetables t
      on (
        {% if target.type == 'snowflake' %}
        (dayofweekiso(d.service_date) between 1 and 5
        {% else %}
        (isodow(d.service_date) between 1 and 5
        {% endif %}
             and split_part(t.calendar_urn, ':', 2) = 'Weekday')
        or (
        {% if target.type == 'snowflake' %}
            dayofweekiso(d.service_date) = 6
        {% else %}
            isodow(d.service_date) = 6
        {% endif %}
            and split_part(t.calendar_urn, ':', 2) in ('Saturday', 'SaturdayHoliday'))
        or (
        {% if target.type == 'snowflake' %}
            dayofweekiso(d.service_date) = 7
        {% else %}
            isodow(d.service_date) = 7
        {% endif %}
            and split_part(t.calendar_urn, ':', 2) in ('Holiday', 'SaturdayHoliday'))
      )

),

-- One timetable per (date, railway, train, direction). A train number
-- legitimately holds several timetables -- Weekday plus SaturdayHoliday, and
-- .1/.2 suffixed variants -- and keeping more than one would emit the same
-- scheduled arrival twice. That is not a harmless duplicate: two identical
-- timestamps make a zero-second scheduled gap, and every ratio downstream
-- divides by it.
picked as (

    select *
    from active
    qualify row_number() over (
        partition by city_key, service_date, railway_urn, train_number, rail_direction_urn
        order by case split_part(calendar_urn, ':', 2)
            when 'Weekday' then 1
            when 'Saturday' then case when iso_dow = 6 then 1 else 3 end
            when 'Holiday' then case when iso_dow = 7 then 1 else 3 end
            when 'SaturdayHoliday' then 2
            else 9 end,
            timetable_urn  -- deterministic tie-break for .1/.2 suffixes
    ) = 1

),

version as (

    select max(gtfs_version_id) as gtfs_version_id
    from {{ ref('stg_gtfs__stops') }}
    where city_key = 'tokyo'

),

exploded as (

    select
        p.city_key,
        p.service_date,
        p.timetable_urn as trip_id,
        map.route_id,
        -- ascending = 0, descending = 1, the same mapping int_odpt_stop_events
        -- writes into fct_stop_events. The two sides join on direction, so the
        -- convention matters far more than matching Toei's own labelling.
        case
            when p.rail_direction_urn = d.ascending_direction then 0
            when p.rail_direction_urn = d.descending_direction then 1
        end as direction_id,
        map.stop_id,
        s.stop_sequence,
        cast(null as {{ dbt.type_int() }}) as timepoint,
        v.gtfs_version_id,
        {{ scheduled_ts_utc('p.service_date', 's.arrival_seconds', "'Asia/Tokyo'") }}
            as sched_arr_ts_utc,
        {{ scheduled_ts_utc('p.service_date', 's.departure_seconds', "'Asia/Tokyo'") }}
            as sched_dep_ts_utc
    from picked p
    join {{ ref('stg_odpt__train_timetable') }} s
      on s.timetable_urn = p.timetable_urn
    join {{ ref('int_odpt_stop_map') }} map
      on map.station_urn = s.station_urn
    join railway_dirs d
      on d.railway_urn = p.railway_urn
    cross join version v

)

select *
from exploded
-- A direction that matches neither the railway's ascending nor its descending
-- URN cannot be placed against the observed side, which drops the same rows via
-- a null stop_pos in int_odpt_stop_events. Keeping them here would add
-- scheduled arrivals that can never pair.
where direction_id is not null
