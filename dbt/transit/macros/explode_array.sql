-- Cross-dialect explode of a JSON-array *string* column (silver stores alert
-- active_periods / informed_entities as JSON text). Emits a FROM-clause fragment:
--
--   from {{ source('silver', 'alerts') }} a
--   {{ explode_json_array('a.informed_entities_json', 'ent') }}
--
-- yielding one row per array element with the element reachable as <alias>.value
-- (JSON in duckdb, VARIANT in Snowflake). Read fields with json_get().
--
-- keep_empty=true rewrites null / '[]' payloads to '[{}]' so the parent row
-- survives as a single all-null-fields element — same effect as an outer flatten,
-- without diverging join syntax between dialects.

{% macro explode_json_array(json_expr, alias, keep_empty=true) %}
    {%- if keep_empty -%}
        {%- set arr = "coalesce(nullif(" ~ json_expr ~ ", '[]'), '[{}]')" -%}
    {%- else -%}
        {%- set arr = "coalesce(" ~ json_expr ~ ", '[]')" -%}
    {%- endif -%}
    {%- if target.type == "snowflake" -%}
        , lateral flatten(input => parse_json({{ arr }})) as {{ alias }}
    {%- else -%}
        , json_each({{ arr }}) as {{ alias }}
    {%- endif -%}
{% endmacro %}


-- Field extraction from one exploded element (<alias>.value). Returns a string-ish
-- scalar in duckdb and a VARIANT in Snowflake; cast at the call site
-- (cast(... as varchar/integer/bigint) is valid over both return types).
{% macro json_get(obj_expr, key) %}
    {%- if target.type == "snowflake" -%}
        get({{ obj_expr }}, '{{ key }}')
    {%- else -%}
        json_extract_string({{ obj_expr }}, '$.{{ key }}')
    {%- endif -%}
{% endmacro %}
