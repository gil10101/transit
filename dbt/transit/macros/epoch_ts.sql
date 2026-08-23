-- Epoch seconds -> naive UTC timestamp, dialect-branched. duckdb to_timestamp
-- yields TIMESTAMPTZ; the cast back to naive is deterministic because the
-- profile pins session TimeZone to UTC.

{% macro epoch_to_ts_utc(epoch_sec) %}
    {%- if target.type == "snowflake" -%}
        to_timestamp_ntz(cast({{ epoch_sec }} as bigint))
    {%- else -%}
        cast(to_timestamp(cast({{ epoch_sec }} as bigint)) as timestamp)
    {%- endif -%}
{% endmacro %}
