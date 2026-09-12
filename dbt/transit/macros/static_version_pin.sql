{% macro static_version_pin(city_col, newest_expr) %}
{#- The static version a model pins to. Normally the newest, which is what the delay
    path wants: the matchers and scheduled stop times only ever work inside the 72h
    window, where newest is correct by definition.

    var('static_version_override') = {city: gtfs_version_id} swaps in an OLDER version
    for a one-off repair run (docs/08, 2026-09-11). A rotation whose calendar starts at
    its publication date leaves the days before it unschedulable against the newest
    static — TTC's 2026-09-06 refresh did that to toronto 09-03..05 — and recomputing
    those days correctly needs the static that was live on them. Scheduled chains never
    set it, so this renders exactly the previous max() pin. -#}
{%- set overrides = var('static_version_override', {}) -%}
{%- if overrides -%}
case {{ city_col }}
{%- for city, version in overrides.items() %}
    when '{{ city }}' then '{{ version }}'
{%- endfor %}
    else {{ newest_expr }} end
{%- else -%}
{{ newest_expr }}
{%- endif -%}
{% endmacro %}
