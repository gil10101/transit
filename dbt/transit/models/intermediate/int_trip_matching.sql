-- All-city RT -> static trip matcher: int_trip_matching_nyc (origin-time token
-- logic) + int_trip_matching_generic (P3 cities), identical columns. This is the
-- only matcher downstream models may reference; the join key downstream is
-- (city_key, service_date, trip_uid) — trip_uid works for HSL's empty trip_ids
-- where trip_id cannot.
--
-- Grain guard: exactly one row per (city_key, service_date, trip_uid). Both
-- inputs already rank their static side, but a duplicate token in static (two
-- active NYC schedule variants sharing an origin-time token, say) could still
-- fan out — keep the best-confidence match deterministically rather than
-- inflating int_stop_events_finalized. tests/assert_trip_matching_unique.sql
-- asserts the guarantee.

with unioned as (

    select city_key, service_date, trip_id, trip_uid, route_id,
           static_trip_id, direction_id, match_confidence
    from {{ ref('int_trip_matching_nyc') }}

    union all

    select city_key, service_date, trip_id, trip_uid, route_id,
           static_trip_id, direction_id, match_confidence
    from {{ ref('int_trip_matching_generic') }}

)

select *
from unioned
qualify row_number() over (
    partition by city_key, service_date, trip_uid
    order by match_confidence desc, static_trip_id
) = 1
