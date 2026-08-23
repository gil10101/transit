-- Custom generic range test (dbt_utils is not installed; mirrors the
-- delay_within_bounds style). Nulls pass; bounds are inclusive unless
-- strict_min flips the lower bound to "must be strictly greater".

{% test value_within_range(model, column_name, min_value=none, max_value=none, strict_min=false) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and (
    false
    {% if min_value is not none %}
    or {{ column_name }} {{ "<=" if strict_min else "<" }} {{ min_value }}
    {% endif %}
    {% if max_value is not none %}
    or {{ column_name }} > {{ max_value }}
    {% endif %}
  )
{% endtest %}
