-- 24 rows, one per LOCAL hour (all analysis is local time — 3am Tokyo compares to
-- 3am NYC, locked rule). daypart comes from macros/daypart.sql so this can never
-- drift from the buckets baked into int_service_frequency / fct_headways / EWT
-- slices; is_peak is the generic daypart-level flag (per-city peak overrides live
-- on dim_city).

{{ config(materialized='table') }}

with hours as (

    {% if target.type == 'snowflake' %}
    select row_number() over (order by seq4()) - 1 as local_hour
    from table(generator(rowcount => 24))
    {% else %}
    select gs.h as local_hour
    from generate_series(0, 23) gs(h)
    {% endif %}

)

select
    local_hour,
    {{ daypart('local_hour') }} as daypart,
    {{ daypart('local_hour') }} in ('am_peak', 'pm_peak') as is_peak
from hours
