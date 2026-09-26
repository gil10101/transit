-- Tokyo's scheduled-trip denominator, at the grain fct_service_delivery_daily
-- needs: (city_key, route_id, service_date) -> trips_scheduled.
--
-- Why this model exists. Every other city's denominator comes from
-- `active_trips` in the delivery mart: static GTFS trips joined to
-- int_service_dates, which expands the published calendar for the service dates
-- seen in `stg_gtfsrt__trip_updates`. Tokyo has no GTFS-RT rail stream — its
-- realtime is odpt:Train JSON — so it contributes no observed dates, no active
-- trips, and trips_scheduled came out 0 on every day. Zero scheduled makes
-- completeness_pct NULL, which makes every Tokyo day unjudgeable, which kept
-- Tokyo out of the scorecard and off the public standings entirely while its
-- stop events, OTP, alerts and vehicle activity were all landing correctly
-- (docs/08, 2026-09-06).
--
-- The schedule universe is odpt:TrainTimetable. A trip is one (railway,
-- train_number) that the day's calendar runs, so the count is over DISTINCT
-- train_number, not timetable_urn: a train number legitimately holds several
-- timetables (Weekday plus SaturdayHoliday, and .1/.2 suffixed variants), and
-- counting rows would multiply the denominator by that fan-out.
--
-- Day-type resolution is deliberately identical to int_odpt_stop_events so the
-- numerator and the denominator agree about what runs on a given date. KNOWN
-- GAP, shared with that model: Japanese national holidays falling on a weekday
-- run holiday schedules but classify as Weekday here. [rev 2026-09-26] It did
-- bite: Tokyo polled until 2026-09-25, so 09-21..23 (Respect for the Aged Day,
-- the bridging holiday, Autumnal Equinox) were expected at weekday service and
-- read ~28% complete. Judged on-time moves 98.05% -> 97.96% without them; not
-- repaired. A jp holiday seed is the fix (docs/01 §C.4, docs/06 Q8).
--
-- Toei only, because int_odpt_stop_map is Toei only (Metro publishes no
-- odpt:Train). NipporiToneri appears in the map but has no realtime at all, so
-- it contributes a denominator with no numerator — that is the honest reading:
-- service we know is scheduled and did not observe.

with dates as (

    -- bounded to service dates Tokyo actually produced realtime for, mirroring
    -- the `observed_dates` bound on the GTFS path so the mart never carries
    -- future-dated rows asserting tomorrow's service was missed
    select distinct city_key, service_date
    from {{ ref('stg_odpt__trains') }}

),

timetables as (

    select distinct railway_urn, train_number, calendar_urn
    from {{ ref('stg_odpt__train_timetable') }}

),

routes as (

    select distinct railway_urn, route_id
    from {{ ref('int_odpt_stop_map') }}

),

active as (

    select
        d.city_key,
        r.route_id,
        d.service_date,
        t.train_number
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
    join routes r on r.railway_urn = t.railway_urn

)

select
    city_key,
    route_id,
    service_date,
    count(distinct train_number) as trips_scheduled
from active
group by city_key, route_id, service_date
