-- Typed interface over silver.vehicle_positions (rename/cast only, mirrors
-- stg_gtfsrt__trip_updates). NYC subway VP carries no lat/lon and no vehicle_id
-- (dictionary quirks); downstream activity models fall back to trip_uid.

select
    city as city_key,
    agency,
    endpoint,
    source_format,
    feed_ts,
    fetched_at,
    service_date,
    -- [rev 2026-08-26] Enforce the identity contract HERE as well as in Spark, so gold
    -- is correct regardless of what silver already holds. A trip_uid is only an identity
    -- if the record carried one; with no trip_id and no route_id the Spark hash collapsed
    -- to a per-city-day constant (twice — see spark_jobs/silver_normalize.py). Silver is
    -- the record of what arrived and keeps its history; gold must not inherit a
    -- fabricated identity from it. assert_trip_uid_is_an_identity guards both sides.
    case
        when trip_id is null and route_id is null then null
        else trip_uid
    end as trip_uid,
    trip_id,
    route_id,
    direction_id,
    start_date,
    start_time,
    schedule_relationship,
    vehicle_id,
    vehicle_label,
    ts_utc,
    lat,
    lon,
    bearing,
    speed,
    current_stop_sequence,
    stop_id,
    current_status,
    occupancy_status
from {{ source('silver', 'vehicle_positions') }}
