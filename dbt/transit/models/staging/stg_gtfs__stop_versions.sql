-- ALL loaded versions of stops.txt — raw material for dim_stop's SCD2 derivation.
--
-- No national filter HERE: stops.txt has no route_id to filter on. The Zurich
-- reduction (national static carries every Swiss stop) happens in dim_stop via
-- int_stops_served, which knows which stops the allow-listed routes actually use.

select
    s.city as city_key,
    s.gtfs_version_id,
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    s.parent_station,
    s.location_type
from {{ source('silver', 'gtfs_static_stops') }} s
