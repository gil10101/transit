-- Which stops each static version actually serves, per (city, version, stop_id) —
-- a stop is "served" when some trip of an allow-listed route stops there.
--
-- Exists for one reason: national feeds. Zurich's stops.txt is every stop in
-- Switzerland, and stops.txt carries no route_id to filter on, so the only way to
-- cut it to the city is through stop_times -> trips -> route allow-list. That walk
-- crosses the largest table in the warehouse (Zurich stop_times: 66.7M rows PER
-- VERSION), hence incremental on gtfs_version_id: each chain run processes only
-- versions it has not seen, and a normal run with no new static scans nothing.
--
-- delete+insert on the version key keeps a re-parsed version (same id, fixed
-- content) from leaving stale stop rows behind.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    unique_key=['city_key', 'gtfs_version_id']
) }}

select distinct
    st.city as city_key,
    st.gtfs_version_id,
    st.stop_id
from {{ source('silver', 'gtfs_static_stop_times') }} st
join {{ source('silver', 'gtfs_static_trips') }} t
  on t.city = st.city
 and t.gtfs_version_id = st.gtfs_version_id
 and t.trip_id = st.trip_id
where {{ national_allowlist_filter('t.city', 't.route_id') }}
{% if is_incremental() %}
  and st.gtfs_version_id not in (select distinct gtfs_version_id from {{ this }})
{% endif %}
