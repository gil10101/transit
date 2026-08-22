-- Timezone semantics implemented once (duckdb dialect; Snowflake variants come with P2).
-- Naive timestamps are UTC unless suffixed _local.

{% macro to_local(ts_utc, tz) %}
    -- input may be naive-UTC (session tz pinned to UTC) or timestamptz; both land
    -- on the same instant, then convert to local wall time
    timezone({{ tz }}, cast({{ ts_utc }} as timestamp with time zone))
{% endmacro %}

-- GTFS noon rule: service_date of an instant = local wall time minus 12h, date part.
{% macro service_date_of(ts_utc, tz) %}
    cast({{ to_local(ts_utc, tz) }} - interval 12 hour as date)
{% endmacro %}

-- Static GTFS time (seconds, may exceed 86400) -> UTC timestamp on a service date.
-- Anchor is local noon minus 12h, which stays correct across DST transitions.
{% macro scheduled_ts_utc(service_date, seconds, tz) %}
    timezone(
        'UTC',
        timezone({{ tz }}, cast({{ service_date }} as timestamp) + interval 12 hour)
        - interval 12 hour
        + to_seconds(cast({{ seconds }} as bigint))
    )
{% endmacro %}
