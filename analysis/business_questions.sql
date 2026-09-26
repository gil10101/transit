-- Transit Pulse — the business questions, as runnable SQL.
--
-- Headline question: which cities run the most reliable public transit, and
-- what makes them reliable? The eight sub-questions below come from
-- docs/transit-pulse-plan.md §1; Q9-Q12 at the end [added 2026-09-26] are the
-- four further cuts the site shows (site/data/extras.json), measured on the
-- standings' judged route-days. Every query here runs against TRANSIT.GOLD in
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
-- [rev 2026-09-26] and to rows with a gap_ratio. Tokyo has a scheduled headway
-- on every gap but gap_ratio/bunched_flag/big_gap_flag are NULL by design
-- (dim_city.gap_regularity_measurable, docs/06 Q4-Q5), and the case-when below
-- turned those NULLs into 0.0% bunched / 0.0% big gap — best in the fleet, for a
-- city whose regularity is not measured. No other city moves: their gap_ratio
-- is non-NULL wherever sched_headway_sec is.
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
  and h.gap_ratio is not null
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
-- [rev 2026-09-20] and only on days the city was still polling. Seven pollers
-- were retired on 2026-09-15/16; an agency keeps publishing next-day trips for
-- days nobody watched, so those route-days arrive scheduled and unobserved and
-- drag the mean down by up to 12 points (Helsinki read 80.7% with them and
-- 93.1% without). That is a fact about the retirement, not about the feed, so
-- `retired_day` — the same flag both completeness tests and the scorecard use —
-- excludes them here too.
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
  and not d.retired_day
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
    where not retired_day          -- [rev 2026-09-20] see Q8
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
      and not retired_day          -- [rev 2026-09-20] see Q8
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
;--split--

-- ---------------------------------------------------------------------------
-- Headline. The composite 0-100 per city, straight from fct_city_scorecard, with
-- the four sub-scores it was built from so a reader can see what each city's
-- number rests on (a NULL sub-score took no part in it). [rev 2026-09-25] All
-- eight cities hold a row: every poller is retired, the last (Tokyo) on 09-25.
-- ---------------------------------------------------------------------------
select c.city_name,
       s.score_0_100,
       s.judged_days,
       s.window_start,
       s.window_end,
       s.s_wait,
       s.s_otp,
       s.s_cancel,
       s.s_bunch
from TRANSIT.GOLD.FCT_CITY_SCORECARD s
join TRANSIT.GOLD.DIM_CITY c on c.city_key = s.city_key
order by s.score_0_100 desc
;--split--


-- ===========================================================================
-- Q9-Q12 [added 2026-09-26]: four cuts the standings cannot show. Same logic
-- as the extras.json block of scripts/export_site_data.py, and the same judged
-- route-day gate as the standings and fct_city_scorecard: closed local day, at
-- or after metrics_from, not a retired_day, completeness >= 0.50. Because of
-- that gate these do not reconcile exactly with Q1/Q2, which read every scored
-- event.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Q9. How late is late — how far each city's delay distribution reaches, not
-- just how much of it lands inside the on-time window. approx_percentile, as
-- the exporter uses, so a re-run can differ from the export by a second. Q2
-- differs more, because of the gate rather than the approximation: NYC's exact
-- median is 4s over every scored event and 15s over judged route-days.
-- ---------------------------------------------------------------------------
with eligible as (
    select d.city_key, d.route_id, d.service_date
    from TRANSIT.GOLD.FCT_SERVICE_DELIVERY_DAILY d
    where d.service_day_closed
      and d.service_date >= d.metrics_from
      and not d.retired_day
      and coalesce(d.completeness_pct, 0) >= 0.50
)
select c.city_name,
       count(*)                                        as events,
       round(approx_percentile(f.delay_arr_sec, 0.50)) as p50_s,
       round(approx_percentile(f.delay_arr_sec, 0.75)) as p75_s,
       round(approx_percentile(f.delay_arr_sec, 0.90)) as p90_s,
       round(approx_percentile(f.delay_arr_sec, 0.95)) as p95_s,
       round(avg(f.delay_arr_sec))                     as mean_s
from TRANSIT.GOLD.FCT_STOP_EVENTS f
join eligible e
  on e.city_key = f.city_key and e.route_id = f.route_id
 and e.service_date = f.service_date
join TRANSIT.GOLD.DIM_CITY c on c.city_key = f.city_key
where f.otp_band is not null
  and f.delay_arr_sec is not null
group by 1
order by p90_s
;--split--

-- ---------------------------------------------------------------------------
-- Q10. Rain. A wet hour has >= 0.5 mm of precipitation (extras.json wet_mm;
-- Q7b's 1 mm cut is kept as it was). Returns city x local_hour x sky cells;
-- the comparison is built from them in Python (export_site_data.py), because
-- rain is not spread evenly over the day: the dry baseline is REWEIGHTED TO THE
-- HOURS IT RAINED, so a city whose showers fell at rush hour is compared with
-- its own dry rush hours, not with a dry average that includes 3am.
--   wet_n   = sum of n over the city's wet cells
--   wet_otp = sum(on_time over wet cells) / wet_n
--   dry_otp = sum over wet hours h of n_wet[h] * on_time_dry[h] / n_dry[h],
--             divided by wet_n (a wet hour with no dry cell adds nothing)
-- Cities with wet_n < 5,000 are omitted — too few rainy arrivals (SF had
-- essentially no rain in its window).
-- ---------------------------------------------------------------------------
with eligible as (
    select d.city_key, d.route_id, d.service_date
    from TRANSIT.GOLD.FCT_SERVICE_DELIVERY_DAILY d
    where d.service_day_closed
      and d.service_date >= d.metrics_from
      and not d.retired_day
      and coalesce(d.completeness_pct, 0) >= 0.50
)
select c.city_name,
       f.local_hour,
       iff(w.precip_mm >= 0.5, 'wet', 'dry')  as sky,
       count(*)                               as n,
       count_if(f.otp_band = 'on_time')       as on_time,
       count(distinct f.local_date)           as dates   -- summed over a city's wet
                                                         -- cells = its rainy hours
from TRANSIT.GOLD.FCT_STOP_EVENTS f
join eligible e
  on e.city_key = f.city_key and e.route_id = f.route_id
 and e.service_date = f.service_date
join TRANSIT.GOLD.FCT_WEATHER_HOURLY w
  on w.city_key = f.city_key and w.local_date = f.local_date
 and w.local_hour = f.local_hour
join TRANSIT.GOLD.DIM_CITY c on c.city_key = f.city_key
where f.otp_band is not null
group by 1, 2, 3
order by 1, 2, 3
;--split--

-- ---------------------------------------------------------------------------
-- Q11. Delay along the trip — median delay by how far along its trip the
-- vehicle is, in tenths: the trip's first scored stop = 0, its last = 1.
-- Does lateness build up along a route, or is it set at the terminal?
-- ---------------------------------------------------------------------------
with eligible as (
    select d.city_key, d.route_id, d.service_date
    from TRANSIT.GOLD.FCT_SERVICE_DELIVERY_DAILY d
    where d.service_day_closed
      and d.service_date >= d.metrics_from
      and not d.retired_day
      and coalesce(d.completeness_pct, 0) >= 0.50
),
scored as (
    select f.city_key, f.service_date, f.trip_uid, f.stop_sequence, f.delay_arr_sec
    from TRANSIT.GOLD.FCT_STOP_EVENTS f
    join eligible e
      on e.city_key = f.city_key and e.route_id = f.route_id
     and e.service_date = f.service_date
    where f.otp_band is not null
      and f.delay_arr_sec is not null
),
trip as (
    select city_key, service_date, trip_uid,
           min(stop_sequence) as lo, max(stop_sequence) as hi
    from scored
    group by 1, 2, 3
),
ev as (
    select s.city_key, s.delay_arr_sec,
           (s.stop_sequence - t.lo) / nullif(t.hi - t.lo, 0) as progress
    from scored s
    join trip t
      on t.city_key = s.city_key and t.service_date = s.service_date
     and t.trip_uid = s.trip_uid
)
select c.city_name,
       cast(least(9, floor(ev.progress * 10)) as int)  as decile,
       count(*)                                         as events,
       round(approx_percentile(ev.delay_arr_sec, 0.5))  as p50_s
from ev
join TRANSIT.GOLD.DIM_CITY c on c.city_key = ev.city_key
where ev.progress is not null            -- single-stop trips have no progress
group by 1, 2
order by 1, 2
;--split--

-- ---------------------------------------------------------------------------
-- Q12. Route spread — the whole distribution of a city's routes rather than
-- one average. A route qualifies with >= 2,000 banded events over its judged
-- route-days; its on-time share is event-weighted across days and directions
-- and rounded to 0.1 as the exporter does. Quantiles follow the site's rule,
-- value = sorted[floor(f * n)] clipped to the last route (site/app.js), so
-- p10 / p90 bound the middle 80% of routes.
-- ---------------------------------------------------------------------------
with eligible as (
    select d.city_key, d.route_id, d.service_date
    from TRANSIT.GOLD.FCT_SERVICE_DELIVERY_DAILY d
    where d.service_day_closed
      and d.service_date >= d.metrics_from
      and not d.retired_day
      and coalesce(d.completeness_pct, 0) >= 0.50
),
routes as (
    select r.city_key, r.route_id,
           round(100 * sum(r.otp_pct * r.banded_events)
               / nullif(sum(case when r.otp_pct is not null then r.banded_events end), 0), 1)
               as otp_pct
    from TRANSIT.GOLD.FCT_ROUTE_RELIABILITY_DAILY r
    join eligible e
      on e.city_key = r.city_key and e.route_id = r.route_id
     and e.service_date = r.service_date
    group by 1, 2
    having sum(r.banded_events) >= 2000
       and sum(case when r.otp_pct is not null then r.banded_events end) > 0
),
ranked as (
    select city_key, otp_pct,
           row_number() over (partition by city_key order by otp_pct) - 1 as i,
           count(*) over (partition by city_key)                          as n
    from routes
)
select c.city_name,
       max(k.n)                                                               as routes,
       max(case when k.i = least(floor(0.1 * k.n), k.n - 1) then k.otp_pct end) as p10_otp,
       max(case when k.i = least(floor(0.5 * k.n), k.n - 1) then k.otp_pct end) as p50_otp,
       max(case when k.i = least(floor(0.9 * k.n), k.n - 1) then k.otp_pct end) as p90_otp,
       round(100.0 * count_if(k.otp_pct >= 80) / max(k.n), 1)                 as routes_ge_80_pct
from ranked k
join TRANSIT.GOLD.DIM_CITY c on c.city_key = k.city_key
group by 1
order by p50_otp desc
;
