with latest as (
    select city, {{ static_version_pin('city', 'max(gtfs_version_id)') }} as gtfs_version_id
    from {{ source('silver', 'gtfs_static_stop_times') }}
    group by 1
)

select
    st.city as city_key,
    st.gtfs_version_id,
    st.trip_id,
    st.stop_id,
    st.stop_sequence,
    st.arrival_time,
    st.departure_time,
    st.arrival_seconds,
    st.departure_seconds,
    -- real on prod since the P3 canonical static projection (MBTA + HSL set it;
    -- NYC and TTC statics omit the column -> null; §D: rail defaults
    -- all-timepoint, bus defaults none). Dev lakes may predate the projection
    -- and duckdb cannot introspect iceberg_scan sources, so dev pins null —
    -- NYC (the only local data) carries no timepoint either way.
    {% if target.type == 'snowflake' %}st.timepoint{% else %}cast(null as int) as timepoint{% endif %}
from {{ source('silver', 'gtfs_static_stop_times') }} st
join latest on st.city = latest.city and st.gtfs_version_id = latest.gtfs_version_id
