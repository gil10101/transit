-- Exploded schedule: one row per (city, service_date, static trip, stop) for the
-- RT-observed service dates. sched_*_ts_utc apply the noon-minus-12h anchor, so
-- >24:00:00 times land on the correct UTC instants across DST.

select
    st.city_key,
    d.service_date,
    t.trip_id,
    t.route_id,
    t.direction_id,
    st.stop_id,
    st.stop_sequence,
    st.timepoint,
    st.gtfs_version_id,
    {{ scheduled_ts_utc('d.service_date', 'st.arrival_seconds', 'c.iana_tz') }} as sched_arr_ts_utc,
    {{ scheduled_ts_utc('d.service_date', 'st.departure_seconds', 'c.iana_tz') }} as sched_dep_ts_utc
from {{ ref('stg_gtfs__trips') }} t
join {{ ref('int_service_dates') }} d
  on d.city_key = t.city_key and d.service_id = t.service_id
join {{ ref('stg_gtfs__stop_times') }} st
  on st.city_key = t.city_key and st.trip_id = t.trip_id
join {{ ref('dim_city') }} c
  on c.city_key = st.city_key
