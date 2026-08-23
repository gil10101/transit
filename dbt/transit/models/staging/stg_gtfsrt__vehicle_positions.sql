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
    trip_uid,
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
