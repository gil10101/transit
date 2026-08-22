with latest as (
    select city, max(gtfs_version_id) as gtfs_version_id
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
    -- NYC subway static omits timepoint; rail defaults all-timepoint (dictionary §D)
    cast(null as int) as timepoint
from {{ source('silver', 'gtfs_static_stop_times') }} st
join latest on st.city = latest.city and st.gtfs_version_id = latest.gtfs_version_id
