-- Contract: scorecard trip totals must equal the route-grain delivery sums for the
-- same eligible route-days. The first scorecard summed trip counts from
-- direction-grain fct_route_reliability_daily, which repeats route-level values on
-- every direction row — double-counting two-direction routes, triple where an
-- unmatched direction folds to -1. This test rebuilds the eligible set at route
-- grain and fails on any city whose published totals drift from it.
--
-- [rev 2026-09-10] City-window grain, matching the scorecard's grain change.
--
-- Dormant while the scorecard is empty (judged_days guard); arms itself the moment
-- the first city is scored. Kept anyway: the alternative is re-noticing the grain
-- trap by hand in a month.

with truth as (

    select
        d.city_key,
        sum(d.trips_scheduled) as scheduled_trips,
        sum(d.trips_observed)  as observed_trips
    from {{ ref('fct_service_delivery_daily') }} d
    where d.service_day_closed
      and d.service_date >= d.metrics_from
      and coalesce(d.completeness_pct, 0) >= {{ var('scorecard_min_completeness') }}
    group by 1

)

select
    s.city_key,
    s.window_start,
    s.window_end,
    s.scheduled_trips as scorecard_scheduled,
    t.scheduled_trips as route_grain_scheduled,
    s.observed_trips  as scorecard_observed,
    t.observed_trips  as route_grain_observed
from {{ ref('fct_city_scorecard') }} s
join truth t
  on t.city_key = s.city_key
where s.scheduled_trips <> t.scheduled_trips
   or s.observed_trips  <> t.observed_trips
