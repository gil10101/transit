-- Timezone semantics implemented once, dialect-branched (duckdb local, Snowflake prod).
-- Naive timestamps are UTC unless suffixed _local.

{% macro to_local(ts_utc, tz) %}
    {%- if target.type == "snowflake" -%}
        convert_timezone('UTC', {{ tz }}, cast({{ ts_utc }} as timestamp_ntz))
    {%- else -%}
        timezone({{ tz }}, cast({{ ts_utc }} as timestamp with time zone))
    {%- endif -%}
{% endmacro %}

-- GTFS noon rule: service_date of an instant = local wall time minus 12h, date part.
{% macro service_date_of(ts_utc, tz) %}
    cast({{ to_local(ts_utc, tz) }} - interval '12 hour' as date)
{% endmacro %}

-- Static GTFS time (seconds, may exceed 86400) -> UTC timestamp on a service date.
-- Anchor is local noon minus 12h, which stays correct across DST transitions.
{% macro scheduled_ts_utc(service_date, seconds, tz) %}
    {%- if target.type == "snowflake" -%}
        dateadd(
            second,
            cast({{ seconds }} as integer),
            convert_timezone(
                {{ tz }}, 'UTC',
                dateadd(hour, 12, cast({{ service_date }} as timestamp_ntz))
            ) - interval '12 hour'
        )
    {%- else -%}
        timezone(
            'UTC',
            timezone({{ tz }}, cast({{ service_date }} as timestamp) + interval 12 hour)
            - interval 12 hour
            + to_seconds(cast({{ seconds }} as bigint))
        )
    {%- endif -%}
{% endmacro %}

-- yyyyMMdd string -> date
{% macro parse_yyyymmdd(col) %}
    {%- if target.type == "snowflake" -%}
        to_date({{ col }}, 'YYYYMMDD')
    {%- else -%}
        strptime({{ col }}, '%Y%m%d')::date
    {%- endif -%}
{% endmacro %}

-- regex capture group extraction
{% macro re_extract(col, pattern, group) %}
    {%- if target.type == "snowflake" -%}
        regexp_substr({{ col }}, '{{ pattern }}', 1, 1, 'e', {{ group }})
    {%- else -%}
        regexp_extract({{ col }}, '{{ pattern }}', {{ group }})
    {%- endif -%}
{% endmacro %}

-- signed seconds between two timestamps (b - a)
{% macro seconds_between(a, b) %}
    {{ dbt.datediff(a, b, "second") }}
{% endmacro %}
