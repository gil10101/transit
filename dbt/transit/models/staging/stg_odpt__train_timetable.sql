-- Latest ODPT train timetable dump (tokyo), stop grain: stop_sequence is the
-- 1-based ordinal of trainTimetableObject; arrival/departure_seconds are
-- GTFS-style seconds since the service day's local midnight (>86400 = past
-- midnight; reconstruction happens in spark_jobs/odpt_static_parse.py because
-- ODPT wraps clock times at 00:00 — docs/01 §C.4). RT join key: odpt:Train's
-- trip_id tail equals timetable_urn's Operator.Line.TrainNumber prefix.

with latest as (
    select city, max(odpt_version_id) as odpt_version_id
    from {{ source('silver', 'odpt_train_timetable') }}
    group by 1
)

select
    t.city as city_key,
    t.odpt_version_id,
    t.timetable_urn,
    t.operator,
    t.railway_urn,
    t.rail_direction as rail_direction_urn,
    t.calendar_urn,
    t.train_number,
    t.train_type as train_type_urn,
    t.train_urn,
    split_part(t.train_urn, ':', 2) as rt_trip_id,
    t.origin_stations_json,
    t.destination_stations_json,
    t.stop_sequence,
    t.station_urn,
    t.arrival_time,
    t.departure_time,
    t.arrival_seconds,
    t.departure_seconds
from {{ source('silver', 'odpt_train_timetable') }} t
join latest on t.city = latest.city and t.odpt_version_id = latest.odpt_version_id
