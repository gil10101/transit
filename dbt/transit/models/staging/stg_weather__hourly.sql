-- Hourly weather per (city, LOCAL date, LOCAL hour) — the Dagster asset already
-- bucketed UTC stamps to each city's local calendar in Python (zoneinfo), so no
-- timezone arithmetic happens here (account session-tz quirk). weather_key is the
-- raw WMO code; dim_weather maps it to a condition bucket.

{% if target.type == 'snowflake' %}
select
    city as city_key,
    local_date,
    local_hour,
    temp_c,
    precip_mm,
    snowfall_cm,
    wind_kph,
    weather_code as weather_key,
    pulled_at
from {{ source('silver', 'weather_hourly') }}
{% else %}
-- weather is written direct-to-Snowflake by the Dagster asset (docs/02: lake
-- unnecessary at 1 row/city/hr), so dev duckdb has no copy: an empty, correctly
-- typed relation keeps the downstream mart and its tests compiling locally.
select
    cast(null as varchar)   as city_key,
    cast(null as date)      as local_date,
    cast(null as integer)   as local_hour,
    cast(null as double)    as temp_c,
    cast(null as double)    as precip_mm,
    cast(null as double)    as snowfall_cm,
    cast(null as double)    as wind_kph,
    cast(null as integer)   as weather_key,
    cast(null as timestamp) as pulled_at
where false
{% endif %}
