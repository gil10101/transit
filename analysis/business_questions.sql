-- Transit Pulse — the business questions, as runnable SQL.
--
-- Headline question: which cities run the most reliable public transit, and
-- what makes them reliable? The eight sub-questions below come from
-- docs/transit-pulse-plan.md §1. Every query here runs against TRANSIT.GOLD in
-- Snowflake and is the exact query whose output is quoted in
-- docs/06-business-answers.md — re-run it to refresh those numbers rather than
-- editing them by hand.
--
-- Run one at a time (statements are separated by the semicolon-dash-dash-split
-- marker the scratch runner keys on; spelling it out here would split this very
-- comment) or paste individually into a worksheet.
--
-- READ THIS BEFORE QUOTING ANY NUMBER:
--   * Only CLOSED local service days at or after a city's dim_city.metrics_from
--     are fair to judge. current_date is UTC; a New York day is still running
--     at 04:00Z. fct_service_delivery_daily.service_day_closed already encodes
--     the local-time rule — use it, do not re-derive it.
--   * delay_arr_sec is NULL wherever a city's schedule cannot be matched
--     (dim_city.schedule_matchable = false). Those events still count as
--     service volume. Divide by count(delay_arr_sec), never by count(*).
--   * ADDED trips count toward volume and never toward OTP (locked rule).


-- ---------------------------------------------------------------------------
-- Q1. On-time performance — the headline. Share of scored stop arrivals in
-- each OTP band per city. Bands come from the otp_bands var: early < -60s,
-- on_time -60..+299s, late 300..899s, very_late 900s+.
-- ---------------------------------------------------------------------------
select c.city_name,
       count(*)                                                                  as scored_events,
       round(100.0 * sum(case when e.otp_band = 'on_time'   then 1 else 0 end) / count(*), 1) as otp_pct,
       round(100.0 * sum(case when e.otp_band = 'early'     then 1 else 0 end) / count(*), 1) as early_pct,
       round(100.0 * sum(case when e.otp_band = 'late'      then 1 else 0 end) / count(*), 1) as late_pct,
       round(100.0 * sum(case when e.otp_band = 'very_late' then 1 else 0 end) / count(*), 1) as very_late_pct
from TRANSIT.GOLD.FCT_STOP_EVENTS e
join TRANSIT.GOLD.DIM_CITY c on c.city_key = e.city_key
where e.otp_band is not null
group by 1
order by otp_pct desc
;--split--

-- Q1b. The same, by local hour — this is the "which city is most reliable at
-- 8am?" query the P6 dashboard is built to answer in two clicks.
select c.city_name, e.local_hour,
       count(*) as events,
       round(100.0 * sum(case when e.otp_band = 'on_time' then 1 else 0 end) / count(*), 1) as otp_pct
from TRANSIT.GOLD.FCT_STOP_EVENTS e
join TRANSIT.GOLD.DIM_CITY c on c.city_key = e.city_key
where e.otp_band is not null
group by 1, 2
having count(*) >= 500          -- thin hours are noise, not signal
order by 1, 2
;--split--


-- ---------------------------------------------------------------------------
-- Q2. Average delay. Signed seconds, positive = late. Median and p90 matter
-- more than the mean: prediction tails drag the mean hard (see NYC below).
-- ---------------------------------------------------------------------------
select c.city_name,
       round(avg(e.delay_arr_sec))                                                  as mean_s,
       round(median(e.delay_arr_sec))                                               as median_s,
       round(percentile_cont(0.90) within group (order by e.delay_arr_sec))         as p90_s,
       round(percentile_cont(0.99) within group (order by e.delay_arr_sec))         as p99_s
from TRANSIT.GOLD.FCT_STOP_EVENTS e
join TRANSIT.GOLD.DIM_CITY c on c.city_key = e.city_key
where e.delay_arr_sec is not null
group by 1
order by median_s
;--split--


-- ---------------------------------------------------------------------------
-- Q3. Service volume — what is actually moving, per mode, at the busiest hour.
-- vehicle_id_reliable is false where the feed does not identify vehicles
-- (NYC subway), so distinct_vehicles there is a floor, not a count.
-- ---------------------------------------------------------------------------
select c.city_name, a.mode,
       max(a.distinct_vehicles)       as peak_vehicles,
       max(a.distinct_trips_active)   as peak_trips,
       max(a.distinct_routes_active)  as peak_routes,
       max(a.vehicle_id_reliable)     as vehicle_id_reliable
from TRANSIT.GOLD.FCT_VEHICLE_ACTIVITY_HOURLY a
join TRANSIT.GOLD.DIM_CITY c on c.city_key = a.city_key
group by 1, 2
order by 1, 2
;--split--


-- ---------------------------------------------------------------------------
-- Q4. Frequency and headways. bunched = actual gap < 0.5x scheduled,
-- big gap = > 2x scheduled. Restricted to rows with a scheduled headway, which
-- is itself gated on dim_city.schedule_matchable.
-- ---------------------------------------------------------------------------
select c.city_name,
       count(*)                                                                as gaps,
       round(avg(h.actual_gap_sec))                                            as mean_gap_s,
       round(median(h.actual_gap_sec))                                         as median_gap_s,
       round(100.0 * avg(case when h.bunched_flag  then 1 else 0 end), 1)      as bunched_pct,
       round(100.0 * avg(case when h.big_gap_flag  then 1 else 0 end), 1)      as big_gap_pct
from TRANSIT.GOLD.FCT_HEADWAYS h
join TRANSIT.GOLD.DIM_CITY c on c.city_key = h.city_key
where h.sched_headway_sec is not null
group by 1
order by bunched_pct desc
;--split--


-- ---------------------------------------------------------------------------
-- Q5. Excess Wait Time — the fair cross-city metric. Only defined for frequent
-- service (scheduled headway <= 10 min, int_service_frequency.is_frequent),
-- where riders turn up without consulting a timetable.
-- ---------------------------------------------------------------------------
select c.city_name,
       count(*)                    as route_days,
       round(avg(r.ewt_sec))       as mean_ewt_s,
       round(median(r.ewt_sec))    as median_ewt_s
from TRANSIT.GOLD.FCT_ROUTE_RELIABILITY_DAILY r
join TRANSIT.GOLD.DIM_CITY c on c.city_key = r.city_key
where r.ewt_sec is not null
  and r.service_date < current_date
group by 1
order by median_ewt_s
;--split--


-- ---------------------------------------------------------------------------
-- Q6. Cancellations and disruptions. Kept as two separate aggregates on
-- purpose: joining alerts to route-days fans out and inflates the cancel rate.
-- ---------------------------------------------------------------------------
select c.city_name,
       round(avg(r.cancel_pct) * 100, 3)  as mean_cancel_pct,
       sum(r.scheduled_trips)             as scheduled_trips
from TRANSIT.GOLD.FCT_ROUTE_RELIABILITY_DAILY r
join TRANSIT.GOLD.DIM_CITY c on c.city_key = r.city_key
group by 1
order by mean_cancel_pct desc
;--split--

select c.city_name,
       sum(a.alerts_active)              as alerts,
       round(sum(a.alert_minutes) / 60)  as alert_hours,
       count(distinct a.route_id)        as routes_alerted
from TRANSIT.GOLD.FCT_ALERTS_DAILY a
join TRANSIT.GOLD.DIM_CITY c on c.city_key = a.city_key
group by 1
order by alerts desc
;--split--


-- ---------------------------------------------------------------------------
-- Q7. Weather sensitivity. LIVE since 2026-08-26: fct_weather_hourly joins
-- fct_stop_events on exactly (city_key, local_date, local_hour) — the explicit
-- contract in docs/02. Two cuts: condition buckets, and OTP per mm of rain.
-- Caveat until the 2yr backfill + accrual widen the sample: few days of summer
-- data means "rain" cells may be thin — n rides along so nobody quotes a
-- 40-event cell as a finding.
-- ---------------------------------------------------------------------------
select c.city_name,
       w.condition_bucket,
       count(e.otp_band) as n_scored,
       round(100.0 * count_if(e.otp_band = 'on_time') / nullif(count(e.otp_band), 0), 1) as otp_pct,
       round(avg(e.delay_arr_sec)) as mean_delay_sec
from TRANSIT.GOLD.FCT_STOP_EVENTS e
join TRANSIT.GOLD.DIM_CITY c on c.city_key = e.city_key
join TRANSIT.GOLD.FCT_WEATHER_HOURLY w
  on w.city_key = e.city_key and w.local_date = e.local_date and w.local_hour = e.local_hour
where e.otp_band is not null
group by 1, 2
having count(e.otp_band) >= 200
order by 1, 2
;--split--
-- Q7b. Dry vs wet, one line per city (wet = >= 1mm/hr while the event happened)
select c.city_name,
       case when w.precip_mm >= 1 then 'wet' else 'dry' end as sky,
       count(e.otp_band) as n_scored,
       round(100.0 * count_if(e.otp_band = 'on_time') / nullif(count(e.otp_band), 0), 1) as otp_pct
from TRANSIT.GOLD.FCT_STOP_EVENTS e
join TRANSIT.GOLD.DIM_CITY c on c.city_key = e.city_key
join TRANSIT.GOLD.FCT_WEATHER_HOURLY w
  on w.city_key = e.city_key and w.local_date = e.local_date and w.local_hour = e.local_hour
where e.otp_band is not null
group by 1, 2
order by 1, 2
;--split--

-- ---------------------------------------------------------------------------
-- Q8. Data completeness — measuring our own sources. Judged only on closed
-- local days at/after metrics_from, and (since 2026-08-25) only on route-days
-- scheduling at least var('completeness_min_trips') trips.
-- ---------------------------------------------------------------------------
select c.city_name,
       count(*)                                as route_days,
       round(100 * avg(d.completeness_pct), 1) as mean_completeness_pct,
       sum(d.trips_scheduled)                  as scheduled,
       sum(d.trips_observed)                   as observed,
       sum(d.trips_added)                      as added,
       sum(d.trips_cancelled)                  as cancelled
from TRANSIT.GOLD.FCT_SERVICE_DELIVERY_DAILY d
join TRANSIT.GOLD.DIM_CITY c on c.city_key = d.city_key
where d.service_day_closed
  and d.service_date >= d.metrics_from
group by 1
order by mean_completeness_pct desc
;--split--

-- Q8b. Where coverage is actually missing, by mode. never_sched isolates dead
-- variant rows in the static (Boston lists 368 bus routes; 215 never run), so
-- sched_not_obs is the honest gap: scheduled service we never saw in realtime.
with sched as (
    select city_key, route_id,
           sum(trips_scheduled) as sched,
           sum(trips_observed)  as obs
    from TRANSIT.GOLD.FCT_SERVICE_DELIVERY_DAILY
    group by 1, 2
)
select r.city_key, r.route_type,
       count(*)                                                                  as static_routes,
       sum(case when coalesce(s.sched, 0) = 0 then 1 else 0 end)                 as never_sched,
       sum(case when s.sched > 0 and coalesce(s.obs, 0) = 0 then 1 else 0 end)   as sched_not_obs,
       sum(case when s.obs > 0 then 1 else 0 end)                                as observed
from TRANSIT.GOLD.STG_GTFS__ROUTES r
left join sched s on s.city_key = r.city_key and s.route_id = r.route_id
group by 1, 2
order by 1, 2
;--split--

-- Q8c. SF only: the 511 feed aggregates ~30 operators under one city, and they
-- do not all publish realtime. Route ids are agency-namespaced ('AC:6', 'SF:N').
with sched as (
    select route_id, sum(trips_scheduled) as sched, sum(trips_observed) as obs
    from TRANSIT.GOLD.FCT_SERVICE_DELIVERY_DAILY
    where city_key = 'sf'
    group by 1
)
select split_part(r.route_id, ':', 1)                        as agency,
       count(*)                                              as routes,
       sum(case when s.sched > 0 then 1 else 0 end)           as scheduled,
       sum(case when s.obs   > 0 then 1 else 0 end)           as observed
from TRANSIT.GOLD.STG_GTFS__ROUTES r
left join sched s on s.route_id = r.route_id
where r.city_key = 'sf'
group by 1
having sum(case when s.sched > 0 then 1 else 0 end) > 0
order by scheduled desc
;
