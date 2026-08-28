-- A trip that id-matched the static must carry a route: the matcher now
-- inherits the static's route_id whenever the feed omits it (SF ADDED trips,
-- 2026-08-27 — a NULL walked into the delivery mart and broke its keys).
-- Rows here mean that inheritance regressed.
select city_key, service_date, trip_uid, static_trip_id
from {{ ref('fct_stop_events') }}
where static_trip_id is not null
  and route_id is null
