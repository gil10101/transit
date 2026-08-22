-- Custom generic test: delays must land in [-30 min, +6 h] (docs/03-dbt-spec.md §7).
-- Failures surface as test rows for investigation; they are never silently dropped.

{% test delay_within_bounds(model, column_name, lower_sec=-1800, upper_sec=21600) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ lower_sec }} or {{ column_name }} > {{ upper_sec }})
{% endtest %}
