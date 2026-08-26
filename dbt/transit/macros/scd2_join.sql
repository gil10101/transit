{#-
  Point-in-time join against an SCD2 dim (valid_to exclusive, NULL = current):
  the interval containing date_col wins. One deliberate extension: a date BEFORE
  the route/stop's first interval falls back to that first interval — several
  cities' first static load postdates their first observed service days by a day
  or two, and the first load is what described the service running then. The
  valid_to bound keeps every match unique; a date inside an absence gap (route
  dropped from the static, later re-added) matches nothing and the key is
  honestly NULL.

  Emits the JOIN clause only; the caller selects <alias>.<key>. Incremental-safe:
  the match is a pure function of the fact row and the dim, so late-arriving
  facts and full refreshes resolve identically.
-#}
{% macro scd2_join(dim_ref, alias, city_col, dim_id_col, fct_id_col, date_col) %}
left join (
    select *,
           min(valid_from) over (partition by city_key, {{ dim_id_col }}) as first_valid_from
    from {{ dim_ref }}
) {{ alias }}
  on {{ alias }}.city_key = {{ city_col }}
 and {{ alias }}.{{ dim_id_col }} = {{ fct_id_col }}
 and ({{ date_col }} >= {{ alias }}.valid_from or {{ alias }}.valid_from = {{ alias }}.first_valid_from)
 and ({{ alias }}.valid_to is null or {{ date_col }} < {{ alias }}.valid_to)
{% endmacro %}
