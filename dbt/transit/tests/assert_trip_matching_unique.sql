-- Matcher grain guard: one RT trip must resolve to at most one static trip, or
-- int_stop_events_finalized silently picks an arbitrary schedule context (and
-- pre-dedup rows multiply the sched-join work). int_trip_matching enforces this
-- with a qualify; this test keeps the guarantee honest.

select city_key, service_date, trip_uid, count(*) as n_matches
from {{ ref('int_trip_matching') }}
group by city_key, service_date, trip_uid
having count(*) > 1
