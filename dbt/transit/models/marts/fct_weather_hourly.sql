-- Weather context for every observed hour. Grain: (city_key, local_date,
-- local_hour) — the EXPLICIT join contract with fct_stop_events (docs/02): join on
-- exactly those three columns, nothing else. condition_bucket rides along so the
-- analysis join is one hop, not two.
--
-- This is context, not reliability: it exists to answer "how much does
-- reliability degrade per mm of rain" (business sub-question 7) and whatever
-- weather questions sit outside the original scope — the optionality principle.
-- Rows exist wherever the collector ran, including hours with no transit
-- observations; that is correct for a context table.

{{ config(materialized='table') }}

select
    md5(concat_ws('|', w.city_key, cast(w.local_date as varchar), w.local_hour)) as weather_hour_key,
    w.city_key,
    w.local_date,
    w.local_hour,
    w.temp_c,
    w.precip_mm,
    w.snowfall_cm,
    w.wind_kph,
    w.weather_key,
    coalesce(d.condition_bucket, 'unknown') as condition_bucket,
    w.pulled_at
from {{ ref('stg_weather__hourly') }} w
left join {{ ref('dim_weather') }} d
       on d.weather_key = w.weather_key
