-- Daypart buckets (P5 locked decision): 5-9 am_peak, 10-15 midday, 16-19 pm_peak,
-- 20-23 evening, 0-4 overnight. Input is a LOCAL hour (0-23) per dim_city.iana_tz.

{% macro daypart(local_hour_col) %}
    case
        when {{ local_hour_col }} between 5 and 9 then 'am_peak'
        when {{ local_hour_col }} between 10 and 15 then 'midday'
        when {{ local_hour_col }} between 16 and 19 then 'pm_peak'
        when {{ local_hour_col }} between 20 and 23 then 'evening'
        else 'overnight'
    end
{% endmacro %}
