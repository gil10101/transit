-- Typed interface over silver.alerts. Silver stores active_periods /
-- informed_entities as JSON text (verified against spark_jobs/silver_normalize.py
-- ALERTS_DDL); both arrays are exploded here so downstream marts see one row per
-- (alert version, active period, informed entity). explode_json_array keeps the
-- parent row when an array is empty/null (all-null element), so alerts with no
-- stated period or entity still flow through.

select
    a.city as city_key,
    a.agency,
    a.endpoint,
    a.source_format,
    a.feed_ts,
    a.fetched_at,
    a.service_date,
    a.alert_id,
    a.cause,
    a.effect,
    a.severity_level,
    a.header_text,
    a.description_text,
    a.content_hash,
    {{ epoch_to_ts_utc(json_get('p.value', 'start')) }} as period_start_ts_utc,
    {{ epoch_to_ts_utc(json_get('p.value', 'end')) }} as period_end_ts_utc,
    cast({{ json_get('e.value', 'agency_id') }} as varchar) as entity_agency_id,
    cast({{ json_get('e.value', 'route_id') }} as varchar) as entity_route_id,
    cast({{ json_get('e.value', 'route_type') }} as integer) as entity_route_type,
    cast({{ json_get('e.value', 'stop_id') }} as varchar) as entity_stop_id,
    cast({{ json_get('e.value', 'trip_id') }} as varchar) as entity_trip_id
from {{ source('silver', 'alerts') }} a
{{ explode_json_array('a.active_periods_json', 'p') }}
{{ explode_json_array('a.informed_entities_json', 'e') }}
