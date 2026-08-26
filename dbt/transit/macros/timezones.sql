-- Timezone semantics implemented once, dialect-branched (duckdb local, Snowflake prod).
-- Naive timestamps are UTC unless suffixed _local.

{% macro to_local(ts_utc, tz) %}
    {%- if target.type == "snowflake" -%}
        convert_timezone('UTC', {{ tz }}, cast({{ ts_utc }} as timestamp_ntz))
    {%- else -%}
        timezone({{ tz }}, cast({{ ts_utc }} as timestamp with time zone))
    {%- endif -%}
{% endmacro %}

-- [rev 2026-08-25] `service_date_of` REMOVED. It was a third implementation of the noon
-- rule, used by no model, and it hard-coded 12h — so it silently disagreed with
-- production for Toronto (4h cutover) by 8 hours. service_date is assigned once, in
-- Spark (spark_jobs/silver_normalize.with_common, constants in spark_jobs/timeutils),
-- and dbt reads it as a column. Do not reintroduce it here: a rule with two
-- implementations has two behaviours.

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

-- regex capture group extraction.
--
-- [rev 2026-08-26] Snowflake string literals treat backslash as an ESCAPE and eat
-- it, so a pattern written '\.\.?([NS])' reached the regex engine as '..?([NS])' —
-- "any two characters then N/S" — which matched the S inside static id prefixes
-- like 'L0S3-…'. Every static 7 trip was labeled southbound, the northbound half
-- of the 7 line matched nothing, and 346 of 661 RT trips silently vanished between
-- silver and gold (2026-08-24; caught by completeness_above_error_50pct). duckdb
-- does not process backslash escapes in plain strings, so dev never saw it. The
-- pattern is doubled for Snowflake here, once, so call sites stay dialect-free.
{% macro re_extract(col, pattern, group) %}
    {%- if target.type == "snowflake" -%}
        regexp_substr({{ col }}, '{{ pattern | replace('\\', '\\\\') }}', 1, 1, 'e', {{ group }})
    {%- else -%}
        regexp_extract({{ col }}, '{{ pattern }}', {{ group }})
    {%- endif -%}
{% endmacro %}

-- signed seconds between two timestamps (b - a)
{% macro seconds_between(a, b) %}
    {{ dbt.datediff(a, b, "second") }}
{% endmacro %}
