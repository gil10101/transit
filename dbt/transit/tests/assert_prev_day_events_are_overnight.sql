-- A stop event assigned to the PREVIOUS service day must have arrived in the small
-- hours. That is what the GTFS noon rule is for: the 00:40 night bus belongs to
-- yesterday's service. A trip filed to yesterday that arrives at 09:34 this morning is
-- not overnight service — it is a misdated row.
--
-- [rev 2026-08-25] This exists because of a real defect. SF operators that omit
-- start_date took the -12h default cutover, so everything fetched before local noon was
-- stamped a day early. That produced a PHANTOM service day: sf/2026-08-23 carried 7,260
-- events of which exactly one genuinely belonged to Aug 23, and
-- fct_route_reliability_daily published an OTP of 65.1% for it computed from trips that
-- ran on Aug 24. Fixed at source by giving sf a 4h cutover (spark_jobs/timeutils.py);
-- this test is the guard so the next city with the same feed shape is caught in one
-- build instead of by a code review.
--
-- The signal is unambiguous. Measured across all cities on 2026-08-25:
--   sf        7,119 previous-day rows, ALL after 06:00, spanning hours 9-20
--   toronto  97,445 previous-day rows, 2 after 06:00
--   helsinki 34,282 rows, hours 0-4      nyc 4,980 rows, hours 0-1
--   boston   14,128 rows, hours 0-4
-- Genuine overnight service clusters in hours 0-4 everywhere. A systematic cutover
-- error shows up as thousands of daytime rows in one city.
--
-- WARN, not error, and deliberately so: the historical SF rows are still in silver (the
-- fix corrects new writes only), so at error severity this would block every chain run
-- for a defect that is already fixed at source. It is also excluded from every scored
-- number — sf.metrics_from is 2026-08-25 and the phantom day is 2026-08-23. Promote to
-- error once the SF backlog is purged or replayed; the threshold below is what makes
-- that safe to do without tripping on the two legitimate Toronto stragglers.
{{ config(severity='warn') }}

select
    city_key,
    count(*) as daytime_prev_day_events,
    min(local_hour) as earliest_hour,
    max(local_hour) as latest_hour
from {{ ref('fct_stop_events') }}
where local_date is not null
  and local_hour is not null
  and datediff('day', service_date, local_date) = 1
  and local_hour >= 6
group by city_key
-- a couple of very late stragglers are plausible; thousands are a broken cutover
having count(*) > 100
